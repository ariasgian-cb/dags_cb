"""
Script otimizado com upload paralelo para GCS e leitura de argumentos.
"""

import os
import time
import zipfile
import requests
import pandas as pd
from typing import List, Set, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pyspark.sql import SparkSession
from google.cloud import storage
from io import StringIO, BytesIO
import logging

# NO TOPO: Adicione a biblioteca para ler argumentos
import argparse

# Configuração para o pool de conexões HTTP (sem alterações)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# TODAS AS SUAS CLASSES (StorageClientSingleton, GCSStateManager, etc.) FICAM AQUI
# NENHUMA MUDANÇA NECESSÁRIA NELAS
# ...
# (Cole suas classes aqui sem alterá-las)
# ...

class StorageClientSingleton:
    """Cliente de Storage compartilhado para evitar múltiplas conexões"""

    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    # Configurar o cliente com pool de conexões otimizado
                    cls._instance._configure_http_session()
                    cls._instance.client = storage.Client()
        return cls._instance

    def _configure_http_session(self):
        """Configura uma sessão HTTP com pool de conexões maior"""
        # Configurar variáveis de ambiente para Google Cloud
        os.environ['GOOGLE_CLOUD_HTTP_POOL_SIZE'] = '20'
        os.environ['GOOGLE_CLOUD_HTTP_POOL_MAXSIZE'] = '20'

        # Configurar configurações do pool urllib3 (versão compatível)
        try:
            import urllib3
            # Configurar pool manager por padrão se estiver disponível
            if hasattr(urllib3, '_default_pool_manager'):
                urllib3._default_pool_manager = None  # Reset para nova configuração

            # Estas variáveis de ambiente são respeitadas pelo google-cloud-storage
            os.environ['GOOGLE_CLOUD_HTTP_MAX_POOL_SIZE'] = '20'

            logger.info("🔧 Pool de conexiones HTTP configurado: maxsize=20")

        except Exception as e:
            logger.warning(f"Não foi possível configurar pool avançado: {e}")
            logger.info("🔧 Usando configuração básica de pool")

    def get_client(self):
        return self.client


class GCSStateManager:
    """Gerencia o estado de processamento no GCS"""

    def __init__(self, bucket_name: str, state_file_path: str):
        self.bucket_name = bucket_name
        self.state_file_path = state_file_path
        self.client = StorageClientSingleton().get_client()
        self.bucket = self.client.bucket(bucket_name)
        self.blob = self.bucket.blob(state_file_path)
        self.lock = Lock()
    
    def load_processed_ids(self) -> Set[str]:
        try:
            if self.blob.exists():
                logger.info(f"📂 Carregando estado desde gs://{self.bucket_name}/{self.state_file_path}")
                content = self.blob.download_as_text()
                df = pd.read_csv(StringIO(content), dtype=str)
                
                if 'CHAVE ACESSO' in df.columns:
                    processed = set(df['CHAVE ACESSO'].dropna().str.strip('"').tolist())
                    logger.info(f"✅ {len(processed)} IDs ya procesados")
                    return processed
            
            logger.info(f"ℹ️ Não há estado prévio")
            return set()
                
        except Exception as e:
            logger.error(f"❌ Erro carregando estado: {e}")
            return set()
    
    def save_processed_ids(self, new_ids: List[str], append: bool = True):
        with self.lock:
            try:
                if append and self.blob.exists():
                    existing_ids = self.load_processed_ids()
                    all_ids = list(existing_ids.union(set(new_ids)))
                else:
                    all_ids = list(set(new_ids))
                
                df = pd.DataFrame(all_ids, columns=['CHAVE ACESSO'])
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                
                self.blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
                
            except Exception as e:
                logger.error(f"❌ Erro salvando estado: {e}")
                raise


class FastGCSUploader:
    """Uploader otimizado com paralelização"""

    def __init__(self, bucket_name: str, max_upload_workers: int = 10):
        self.client = StorageClientSingleton().get_client()
        self.bucket = self.client.bucket(bucket_name)
        self.max_upload_workers = max_upload_workers
    
    def upload_single_file(self, file_data: Dict) -> bool:
        """Faz upload de um arquivo individual para GCS"""
        try:
            blob_path = file_data['blob_path']
            content = file_data['content']
            
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(content, content_type='application/xml')
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro fazendo upload {file_data['blob_path']}: {e}")
            return False
    
    def upload_files_parallel(self, files_data: List[Dict]) -> int:
        """Faz upload de múltiplos arquivos em paralelo"""
        uploaded = 0
        
        with ThreadPoolExecutor(max_workers=self.max_upload_workers) as executor:
            futures = [executor.submit(self.upload_single_file, file_data) 
                      for file_data in files_data]
            
            for future in as_completed(futures):
                if future.result():
                    uploaded += 1
        
        return uploaded


class APIDownloaderVPC:
    """Downloader otimizado com upload paralelo para GCS"""
    
    def __init__(self, max_workers: int = 3, batch_size: int = 500, 
                 max_upload_workers: int = 10):
        self.max_workers = max_workers
        self.base_url = "https://k1-documentos-fiscais.viavarejo.com.br/v1/documentosFiscais"
        self.batch_size = batch_size
        self.max_upload_workers = max_upload_workers
        
    def download_batch(self, batch_ids: List[str], batch_number: int) -> dict:
        """Baixa um lote de IDs"""
        headers = {
            'Accept': 'application/zip',
            'Content-Type': 'application/json'
        }
        
        result = {
            'batch_number': batch_number,
            'success': False,
            'ids': batch_ids,
            'file_path': None,
            'error': None,
            'download_duration': 0,
            'file_size_mb': 0
        }
        
        try:
            logger.info(f"🔄 Lote {batch_number}: Baixando {len(batch_ids)} registros...")
            start_time = time.time()
            
            response = requests.post(
                f"{self.base_url}/zip-xml-completo",
                json=batch_ids,
                headers=headers,
                timeout=300
            )
            
            download_duration = time.time() - start_time
            result['download_duration'] = download_duration
            
            if response.status_code == 200:
                file_path = f'/tmp/batch_{batch_number}_{int(time.time())}.zip'
                
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                file_size_mb = len(response.content) / (1024 * 1024)
                result['file_size_mb'] = file_size_mb
                result['file_path'] = file_path
                result['success'] = True
                
                logger.info(f"✅ Lote {batch_number}: {file_size_mb:.2f} MB baixado em {download_duration:.2f}s")
                
            else:
                result['error'] = f"HTTP {response.status_code}"
                logger.error(f"❌ Lote {batch_number}: {result['error']}")
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Lote {batch_number}: {e}")
        
        return result
    
    def extract_and_upload_parallel(self, zip_path: str, gcs_bucket: str,
                                   gcs_prefix: str, batch_number: int) -> Dict:
        """
        Extrai XMLs do ZIP e os envia em paralelo para GCS
        ⚡ OTIMIZAÇÃO: Upload paralelo em vez de sequencial
        """
        stats = {
            'success': False,
            'xml_uploaded': 0,
            'total_files_in_zip': 0,
            'extract_duration': 0,
            'upload_duration': 0,
            'errors': []
        }
        
        try:
            # 1. Extrair todos os XMLs do ZIP primeiro
            extract_start = time.time()
            files_to_upload = []
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.filelist
                stats['total_files_in_zip'] = len(file_list)
                
                for file_info in file_list:
                    try:
                        if file_info.is_dir() or file_info.file_size == 0:
                            continue
                        
                        filename = file_info.filename
                        ext = os.path.splitext(filename)[1].lower()
                        
                        if ext == '.xml':
                            with zip_ref.open(file_info) as file:
                                content = file.read()
                            
                            file_basename = os.path.basename(filename)
                            blob_path = f"{gcs_prefix}/{file_basename}"
                            
                            files_to_upload.append({
                                'blob_path': blob_path,
                                'content': content,
                                'size': file_info.file_size
                            })
                        
                    except Exception as e:
                        stats['errors'].append(f"{file_info.filename}: {e}")
            
            stats['extract_duration'] = time.time() - extract_start
            
            # 2. Fazer upload de todos os arquivos EM PARALELO
            if files_to_upload:
                upload_start = time.time()
                
                uploader = FastGCSUploader(gcs_bucket, self.max_upload_workers)
                uploaded_count = uploader.upload_files_parallel(files_to_upload)
                
                stats['upload_duration'] = time.time() - upload_start
                stats['xml_uploaded'] = uploaded_count
                
                if uploaded_count > 0:
                    stats['success'] = True
                    total_size_mb = sum(f['size'] for f in files_to_upload) / (1024 * 1024)
                    
                    logger.info(f"📤 Lote {batch_number}:")
                    logger.info(f"   ⚙️  Extração: {stats['extract_duration']:.2f}s")
                    logger.info(f"   ☁️  Upload paralelo: {stats['upload_duration']:.2f}s")
                    logger.info(f"   📄 {uploaded_count} XMLs ({total_size_mb:.2f} MB)")
                    logger.info(f"   🚀 Velocidade: {uploaded_count/stats['upload_duration']:.1f} arquivos/s")
            
            # 3. Limpar arquivo temporário
            os.remove(zip_path)
            
            return stats
            
        except Exception as e:
            stats['errors'].append(f"Error: {e}")
            logger.error(f"❌ Lote {batch_number}: {e}")
            return stats
    
    def download_parallel(self, all_ids: List[str], processed_ids: Set[str],
                         gcs_bucket: str, gcs_prefix: str,
                         state_manager: GCSStateManager) -> Dict:
        """Baixa múltiplos lotes em paralelo"""
        
        pending_ids = [id for id in all_ids if id not in processed_ids]
        
        if not pending_ids:
            logger.info("✅ Não há IDs pendentes")
            # ⚠️ ADICIONAR ESSAS LINHAS:
            return {
                'failed_batches': [], 
                'total_xml_uploaded': 0,
                'download_time': 0,      # ⬅️ ADICIONAR
                'upload_time': 0         # ⬅️ ADICIONAR
            }
        
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 CONFIGURACIÓN OPTIMIZADA:")
        logger.info(f"   Total IDs: {len(all_ids):,}")
        logger.info(f"   Já processados: {len(processed_ids):,}")
        logger.info(f"   Pendientes: {len(pending_ids):,}")
        
        batches = []
        for i in range(0, len(pending_ids), self.batch_size):
            batch = pending_ids[i:i + self.batch_size]
            batches.append((batch, i // self.batch_size + 1))
        
        total_batches = len(batches)
        logger.info(f"🔢 Lotes: {total_batches}")
        logger.info(f"⚡ Workers download: {self.max_workers}")
        logger.info(f"☁️  Workers upload GCS: {self.max_upload_workers}")
        logger.info(f"{'='*70}\n")
        
        completed = 0
        failed = []
        total_xml = 0
        total_download_time = 0
        total_upload_time = 0
        
        overall_start = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self.download_batch, batch_ids, batch_num): (batch_ids, batch_num)
                for batch_ids, batch_num in batches
            }
            
            for future in as_completed(future_to_batch):
                result = future.result()
                completed += 1
                
                if result['success']:
                    total_download_time += result['download_duration']
                    
                    stats = self.extract_and_upload_parallel(
                        result['file_path'], 
                        gcs_bucket, 
                        gcs_prefix,
                        result['batch_number']
                    )
                    
                    if stats['success'] and stats['xml_uploaded'] > 0:
                        total_xml += stats['xml_uploaded']
                        total_upload_time += stats['upload_duration']
                        
                        state_manager.save_processed_ids(result['ids'], append=True)
                        
                        logger.info(f"✅ Lote {result['batch_number']}/{total_batches} completado")
                        logger.info(f"   📊 Progreso: {completed}/{total_batches} | {total_xml:,} XMLs\n")
                    else:
                        failed.append(result['batch_number'])
                else:
                    failed.append(result['batch_number'])
        
        overall_duration = time.time() - overall_start
        
        # Análise detalhada
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 ANÁLISE DE RENDIMENTO:")
        logger.info(f"{'='*70}")
        logger.info(f"✅ Lotes bem-sucedidos: {completed - len(failed)}/{total_batches}")
        logger.info(f"❌ Lotes falhados: {len(failed)}")
        logger.info(f"📄 XMLs procesados: {total_xml:,}")
        logger.info(f"")
        logger.info(f"⏱️  DISTRIBUIÇÃO DE TEMPO:")
        logger.info(f"   Tempo total: {overall_duration/60:.2f} minutos")
        logger.info(f"   Tempo em downloads API: {total_download_time/60:.2f} min ({total_download_time/overall_duration*100:.1f}%)")
        logger.info(f"   Tempo em uploads GCS: {total_upload_time/60:.2f} min ({total_upload_time/overall_duration*100:.1f}%)")
        logger.info(f"   Overhead (processamento): {(overall_duration-total_download_time-total_upload_time)/60:.2f} min")
        logger.info(f"")
        logger.info(f"🚀 VELOCIDADES:")
        if completed > 0 and total_download_time > 0 and total_upload_time > 0:
            logger.info(f"   Velocidade download: {total_xml/total_download_time:.1f} XMLs/segundo")
            logger.info(f"   Velocidade upload: {total_xml/total_upload_time:.1f} XMLs/segundo")
            logger.info(f"   Velocidade geral: {total_xml/overall_duration:.1f} XMLs/segundo")
        logger.info(f"")
        logger.info(f"⏱️  PROJEÇÃO PARA 2M REGISTROS:")
        avg_time_per_xml = overall_duration / total_xml if total_xml > 0 else 0
        total_time_2M_hours = (avg_time_per_xml * 2000000) / 3600
        logger.info(f"   Tempo estimado: {total_time_2M_hours:.1f} horas")
        logger.info(f"{'='*70}\n")
        
        return {
            'failed_batches': failed,
            'total_xml_uploaded': total_xml,
            'download_time': total_download_time,
            'upload_time': total_upload_time
        }


def load_ids_from_gcs_csv(bucket_name: str, csv_path: str) -> List[str]:
    storage_client = StorageClientSingleton().get_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(csv_path)
    
    logger.info(f"📂 Carregando IDs desde gs://{bucket_name}/{csv_path}")
    content = blob.download_as_text()
    df = pd.read_csv(StringIO(content), dtype=str, quotechar='"',skipinitialspace=True)
    
    if 'CHAVE ACESSO' in df.columns:
        ids = df['CHAVE ACESSO'].str.strip('"').dropna().tolist()
        logger.info(f"✅ {len(ids):,} IDs carregados")
        return ids
    else:
        raise ValueError("Coluna 'CHAVE ACESSO' não encontrada")


def main_dataproc():
    """Função principal otimizada que lê argumentos"""
    
    # --- NOVO: LER ARGUMENTOS DA LINHA DE COMANDO ---
    parser = argparse.ArgumentParser(description="Dataproc job para download de XMLs.")
    parser.add_argument("--gcs_bucket", required=True, help="Nome do GCS bucket.")
    parser.add_argument("--source_csv", required=True, help="Caminho para o CSV de origem no bucket.")
    parser.add_argument("--state_file", required=True, help="Caminho para o arquivo de estado no bucket.")
    parser.add_argument("--output_prefix", required=True, help="Prefixo de saída para os XMLs no bucket.")
    parser.add_argument("--max_workers", type=int, default=5, help="Número de workers para download.")
    parser.add_argument("--batch_size", type=int, default=500, help="Tamanho do lote para a API.")
    parser.add_argument("--max_upload_workers", type=int, default=8, help="Número de workers para upload.")
    
    args = parser.parse_args()

    # --- REMOVER OS OS.GETENV E USAR OS ARGUMENTOS LIDOS ---
    GCS_BUCKET = args.gcs_bucket
    GCS_SOURCE_CSV = args.source_csv
    GCS_STATE_FILE = args.state_file
    GCS_OUTPUT_PREFIX = args.output_prefix
    MAX_WORKERS = args.max_workers
    BATCH_SIZE = args.batch_size
    MAX_UPLOAD_WORKERS = args.max_upload_workers

    logger.info("🚀 INICIANDO DATAPROC CON UPLOAD PARALELO")
    logger.info(f"   Bucket: gs://{GCS_BUCKET}")
    logger.info(f"   Workers descarga: {MAX_WORKERS}")
    logger.info(f"   Workers upload: {MAX_UPLOAD_WORKERS}\n")
    
    spark = SparkSession.builder.appName("XML_Downloader_Fast").getOrCreate()
    
    try:
        state_manager = GCSStateManager(GCS_BUCKET, GCS_STATE_FILE)
        all_ids = load_ids_from_gcs_csv(GCS_BUCKET, GCS_SOURCE_CSV)
        processed_ids = state_manager.load_processed_ids()
        
        downloader = APIDownloaderVPC(
            max_workers=MAX_WORKERS, 
            batch_size=BATCH_SIZE,
            max_upload_workers=MAX_UPLOAD_WORKERS
        )
        
        result = downloader.download_parallel(
            all_ids, processed_ids, GCS_BUCKET, GCS_OUTPUT_PREFIX, state_manager
        )
        
        logger.info(f"✅ PROCESO COMPLETADO")
        logger.info(f"   XMLs: {result['total_xml_uploaded']:,}")
        if result.get('download_time', 0) > 0:
            logger.info(f"   Tempo download: {result['download_time']/60:.1f} min")
            logger.info(f"   Tempo upload: {result['upload_time']/60:.1f} min")
        
    except Exception as e:
        logger.error(f"❌ ERROR: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main_dataproc()
