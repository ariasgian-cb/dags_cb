"""
Data: 2026-03-17
Autor: Claude Code
Descrição: Gera causa_raiz e correção para todos os 226 erros e salva no bucket GCS
Refatorado para Dataproc Serverless em 2026-04-07
"""

import csv
import time
from datetime import datetime
import argparse
import google.genai as genai
import pandas as pd
from tqdm import tqdm
import hashlib

# ============================================
# ARGUMENTOS (substitui dotenv)
# ============================================
parser = argparse.ArgumentParser(description='Gera respostas IA para erros tributários')
parser.add_argument("--project_id", required=True, help="ID do projeto GCP")
parser.add_argument("--location", required=True, help="Região do Vertex AI")
parser.add_argument("--gemini_model", required=True, help="Nome do modelo Gemini")
parser.add_argument("--bucket_file_path", required=True, help="Caminho GCS da base de conhecimento")
parser.add_argument("--output_bucket_path", required=True, help="Caminho GCS para outputs")
parser.add_argument("--bigquery_dataset", required=True, help="Dataset BigQuery")
parser.add_argument("--bigquery_table", required=True, help="Tabela BigQuery histórico")
parser.add_argument("--bigquery_table_feedback", required=True, help="Tabela BigQuery feedbacks")
parser.add_argument("--table_errors_path", required=True, help="Caminho GCS do arquivo table_error_new.txt")
args = parser.parse_args()

# Variáveis globais
PROJECT_ID = args.project_id
LOCATION = args.location
GEMINI_MODEL = args.gemini_model
BUCKET_FILE_PATH = args.bucket_file_path
OUTPUT_BUCKET_PATH = args.output_bucket_path
BIGQUERY_DATASET = args.bigquery_dataset
BIGQUERY_TABLE = args.bigquery_table
BIGQUERY_TABLE_FEEDBACK = args.bigquery_table_feedback
TABLE_ERRORS_PATH = args.table_errors_path

# Credenciais usando Application Default Credentials (ADC)
from google.auth import default
creds, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])


def init_gemini():
    """Inicializa o cliente Gemini"""
    if not PROJECT_ID:
        raise ValueError("PROJECT_ID não encontrado nas variáveis de ambiente")

    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION,
        credentials=creds
    )
    return client


def load_csv_from_bucket(gs_path):
    """Carrega CSV completo do bucket (base de conhecimento)"""
    try:
        df = pd.read_csv(
            gs_path,
            sep=';',
            storage_options={"token": creds}
        )
        print(f"✅ Base de conhecimento carregada: {len(df)} erros mapeados")
        return df
    except Exception as e:
        print(f"⚠️ Erro ao carregar CSV do bucket: {str(e)}")
        return pd.DataFrame()


def load_table_errors():
    """Carrega todos os 226 erros do GCS"""
    try:
        df = pd.read_csv(
            TABLE_ERRORS_PATH,  # Agora carrega do GCS
            storage_options={"token": creds}
        )
        print(f"✅ Table errors carregado: {len(df)} erros totais")
        return df
    except Exception as e:
        print(f"⚠️ Erro ao carregar table_errors: {str(e)}")
        return pd.DataFrame()


def load_respostas_consolidadas():
    """
    Carrega erros CONSOLIDADOS do BigQuery (10+ feedbacks positivos)
    Estes erros não precisam mais ser regenerados
    """
    try:
        from google.cloud import bigquery

        client_bq = bigquery.Client(credentials=creds, project=PROJECT_ID)

        query = f"""
        WITH feedbacks_agregados AS (
            SELECT
                id_error,
                inconsistencia,
                causa_raiz_gerada,
                correcao_gerada,
                COUNT(*) as total_feedbacks,
                SUM(CASE WHEN flag_causa_raiz_util = TRUE AND flag_correcao_util = TRUE THEN 1 ELSE 0 END) as feedbacks_positivos
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FEEDBACK}`
            GROUP BY id_error, inconsistencia, causa_raiz_gerada, correcao_gerada
        )
        SELECT
            id_error,
            inconsistencia as description,
            causa_raiz_gerada as causa_raiz,
            correcao_gerada as correcao,
            feedbacks_positivos
        FROM feedbacks_agregados
        WHERE feedbacks_positivos >= 10
        """

        df = client_bq.query(query).to_dataframe()
        print(f"✅ Respostas CONSOLIDADAS carregadas: {len(df)} erros (10+ feedbacks positivos)")
        return df

    except Exception as e:
        print(f"⚠️ Nenhuma resposta consolidada encontrada: {str(e)}")
        return pd.DataFrame()


def load_respostas_refinadas():
    """Carrega respostas refinadas do bucket (geradas pelo script 3)"""
    try:
        # Busca arquivo mais recente de base refinada
        from google.cloud import storage

        client_storage = storage.Client(credentials=creds)
        bucket_name = OUTPUT_BUCKET_PATH.split('//')[1].split('/')[0]
        prefix = '/'.join(OUTPUT_BUCKET_PATH.split('/')[3:])

        bucket = client_storage.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        # Filtra apenas arquivos base_refinada_*.csv
        refinados = [blob for blob in blobs if 'base_refinada_' in blob.name and blob.name.endswith('.csv')]

        if not refinados:
            print(f"⚠️ Nenhuma base refinada encontrada (primeira execução)")
            return pd.DataFrame()

        # Pega o mais recente
        refinado_mais_recente = max(refinados, key=lambda x: x.time_created)

        df = pd.read_csv(
            f"gs://{bucket_name}/{refinado_mais_recente.name}",
            sep=',',
            storage_options={"token": creds}
        )
        print(f"✅ Respostas refinadas carregadas: {len(df)} erros (arquivo: {refinado_mais_recente.name})")
        return df
    except Exception as e:
        print(f"⚠️ Nenhuma base refinada encontrada: {str(e)}")
        return pd.DataFrame()


def gerar_causa_correcao_para_erro(client, error_row, df_base_conhecimento, df_respostas_refinadas):
    """
    Gera causa_raiz e correção para um erro específico usando inferência por padrões
    """
    error_description = error_row['description']
    error_id = error_row['id_error']
    error_regra = error_row['id_regra']

    # Monta contexto com exemplos da base de conhecimento
    context_exemplos = "=== BASE DE CONHECIMENTO ===\n\n"

    for idx, (_, row) in enumerate(df_base_conhecimento.head(30).iterrows(), 1):
        context_exemplos += f"Exemplo {idx}:\n"
        context_exemplos += f"- ID: {row['id_error']}\n"
        context_exemplos += f"- Descrição: {row['description']}\n"
        context_exemplos += f"- Regra: {row['id_regra']}\n"
        context_exemplos += f"- Causa Raiz: {row['causa_raiz']}\n"
        context_exemplos += f"- Correção: {row['correcao']}\n\n"

    # Adiciona contexto de respostas refinadas (aprendizado)
    context_refinado = ""
    if not df_respostas_refinadas.empty:
        context_refinado = "\n=== RESPOSTAS REFINADAS COM FEEDBACK (APRENDIZADO) ===\n\n"
        context_refinado += "IMPORTANTE: Use estes exemplos como referência de qualidade.\n"
        context_refinado += "Estas respostas foram refinadas com sugestões de usuários especialistas.\n\n"

        for idx, refinado in df_respostas_refinadas.head(10).iterrows():
            context_refinado += f"Exemplo Refinado {idx + 1}:\n"
            context_refinado += f"- ID Erro: {refinado['id_error']}\n"
            context_refinado += f"- Descrição: {refinado['description']}\n"
            context_refinado += f"- Causa Raiz (REFINADA): {refinado['causa_raiz']}\n"
            context_refinado += f"- Correção (REFINADA): {refinado['correcao']}\n\n"

    prompt = f"""
Você é um especialista em erros fiscais tributários brasileiros.

{context_exemplos}

{context_refinado}

=== NOVO ERRO PARA MAPEAR ===
ID: {error_id}
Descrição: {error_description}
Regra: {error_regra}

TAREFA:
Analise os EXEMPLOS e FEEDBACKS acima e identifique padrões:
- Erros de "não encontrado no PostgreSQL" → causa de sincronização de dados
- Erros de "divergente XML vs PostgreSQL" → causa de cálculo ou parsing incorreto
- Erros de "CST/CFOP inválido" → causa de configuração tributária
- Se houver FEEDBACK para este erro, PRIORIZE a sugestão do usuário

Com base nos padrões aprendidos, INFIRA:

1. CAUSA RAIZ: Descreva a causa provável do erro (2-3 frases objetivas)
2. CORREÇÃO: Passo a passo para corrigir (formato de lista numerada)

FORMATO DE RESPOSTA (obrigatório):
CAUSA_RAIZ: [sua análise da causa]
CORRECAO: [passo a passo para resolver]
CONFIANCA: [ALTA/MEDIA/BAIXA]
"""

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )

        texto = response.text.strip()

        # Parse da resposta
        causa_raiz = ""
        correcao = ""
        confianca = "BAIXA"

        if "CAUSA_RAIZ:" in texto:
            causa_parte = texto.split("CAUSA_RAIZ:")[1].split("CORRECAO:")[0].strip()
            causa_raiz = causa_parte

        if "CORRECAO:" in texto:
            correcao_parte = texto.split("CORRECAO:")[1].split("CONFIANCA:")[0].strip()
            correcao = correcao_parte

        if "CONFIANCA:" in texto:
            confianca_parte = texto.split("CONFIANCA:")[1].strip()
            confianca = confianca_parte.split()[0]

        return {
            "causa_raiz": causa_raiz,
            "correcao": correcao,
            "confianca": confianca,
            "origem": "INFERIDO"
        }

    except Exception as e:
        print(f"   ⚠️ Erro ao gerar para ID {error_id}: {str(e)}")
        return {
            "causa_raiz": "ERRO_GERACAO",
            "correcao": "ERRO_GERACAO",
            "confianca": "ERRO",
            "origem": "ERRO"
        }


def calcular_hash(causa_raiz, correcao):
    """Calcula hash MD5 para detectar mudanças"""
    texto = f"{causa_raiz}|{correcao}"
    return hashlib.md5(texto.encode()).hexdigest()


def main():
    # Marca início da execução
    tempo_inicio = time.time()

    print("=" * 80)
    print("🤖 SCRIPT 1: GERADOR EM LOTE COM HISTÓRICO")
    print("📋 Gera causa_raiz e correção para todos os 226 erros")
    print("=" * 80)
    print()

    # Inicializa cliente
    print("⚙️  Inicializando Gemini...")
    client = init_gemini()
    print("✅ Cliente inicializado\n")

    # Carrega bases
    print("📂 Carregando bases de dados...")
    df_base_conhecimento = load_csv_from_bucket(BUCKET_FILE_PATH)
    df_todos_erros = load_table_errors()
    df_respostas_consolidadas = load_respostas_consolidadas()
    df_respostas_refinadas = load_respostas_refinadas()
    print()

    if df_base_conhecimento.empty or df_todos_erros.empty:
        print("❌ Erro: Não foi possível carregar as bases de dados")
        return

    # Prepara DataFrame de output
    output_data = []
    data_execucao = datetime.now()
    data_geracao = data_execucao.strftime("%Y-%m-%d")
    versao = f"v{data_execucao.strftime('%Y%m%d_%H%M%S')}"

    print("🔄 Processando erros...\n")
    print(f"Total de erros a processar: {len(df_todos_erros)}")
    print(f"Erros já mapeados: {len(df_base_conhecimento)}")
    print(f"Respostas consolidadas (10+ feedbacks): {len(df_respostas_consolidadas)}")
    print(f"Respostas refinadas disponíveis: {len(df_respostas_refinadas)}")
    print()

    # Cria dicionário dos erros mapeados (base original)
    erros_mapeados = {}
    for _, row in df_base_conhecimento.iterrows():
        erros_mapeados[row['id_error']] = {
            'causa_raiz': row['causa_raiz'],
            'correcao': row['correcao'],
            'origem': 'MAPEADO',
            'confianca': 'ALTA'
        }

    # Cria dicionário das respostas CONSOLIDADAS (PRIORIDADE MÁXIMA)
    erros_consolidados = {}
    if not df_respostas_consolidadas.empty:
        for _, row in df_respostas_consolidadas.iterrows():
            erros_consolidados[row['id_error']] = {
                'causa_raiz': row['causa_raiz'],
                'correcao': row['correcao'],
                'origem': 'CONSOLIDADO',
                'confianca': 'MUITO_ALTA'
            }

    # Cria dicionário das respostas refinadas (PRIORIDADE ALTA)
    erros_refinados = {}
    if not df_respostas_refinadas.empty:
        for _, row in df_respostas_refinadas.iterrows():
            erros_refinados[row['id_error']] = {
                'causa_raiz': row['causa_raiz'],
                'correcao': row['correcao'],
                'origem': 'REFINADO_FEEDBACK',
                'confianca': 'ALTA'
            }

    # Processa cada erro
    for _, row in tqdm(df_todos_erros.iterrows(), total=len(df_todos_erros), desc="Processando"):
        error_id = row['id_error']

        # PRIORIDADE 1: Respostas CONSOLIDADAS (10+ feedbacks positivos - não precisa mais gerar)
        if error_id in erros_consolidados:
            resultado = erros_consolidados[error_id]
            print(f"  ⭐⭐ ID {error_id}: CONSOLIDADO (10+ feedbacks positivos)")
        # PRIORIDADE 2: Respostas refinadas (feedbacks processados)
        elif error_id in erros_refinados:
            resultado = erros_refinados[error_id]
            print(f"  ⭐ ID {error_id}: REFINADO (com feedback)")
        # PRIORIDADE 3: Base original (mapeado)
        elif error_id in erros_mapeados:
            resultado = erros_mapeados[error_id]
            print(f"  ✅ ID {error_id}: MAPEADO")
        # PRIORIDADE 4: Gera com IA
        else:
            print(f"  🧠 ID {error_id}: Gerando com IA...")
            resultado = gerar_causa_correcao_para_erro(
                client,
                row,
                df_base_conhecimento,
                df_respostas_refinadas
            )

        # Calcula hash da resposta
        hash_resposta = calcular_hash(resultado['causa_raiz'], resultado['correcao'])

        # Adiciona ao output
        output_data.append({
            'id_error': row['id_error'],
            'description': row['description'],
            'id_regra': row['id_regra'],
            'causa_raiz': resultado['causa_raiz'],
            'correcao': resultado['correcao'],
            'data_geracao': data_geracao,
            'data_execucao': data_execucao.isoformat(),
            'versao': versao,
            'origem': resultado['origem'],
            'confianca': resultado['confianca'],
            'hash_resposta': hash_resposta
        })

    # Cria DataFrame final
    df_output = pd.DataFrame(output_data)

    # Salva no bucket com timestamp
    output_filename = f"CSV_output_{data_geracao}.csv"
    output_path = OUTPUT_BUCKET_PATH.rstrip('/') + '/' + output_filename

    # Salva CSV com vírgula como separador
    # QUOTE_MINIMAL = coloca aspas apenas quando necessário (ex: campo contém vírgula)
    df_output.to_csv(
        output_path,
        sep=',',  # Usa vírgula como delimitador
        index=False,
        encoding='utf-8',
        quoting=csv.QUOTE_MINIMAL,  # Aspas apenas quando necessário (padrão)
        storage_options={"token": creds}
    )

    # Calcula tempo de execução
    tempo_fim = time.time()
    tempo_total = tempo_fim - tempo_inicio
    tempo_formatado = time.strftime("%H:%M:%S", time.gmtime(tempo_total))

    print("\n" + "=" * 80)
    print("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
    print("=" * 80)
    print(f"\n📊 ESTATÍSTICAS:")
    print(f"   Total de erros processados: {len(df_output)}")
    print(f"   Erros CONSOLIDADOS (10+ feedbacks): {len(df_output[df_output['origem'] == 'CONSOLIDADO'])}")
    print(f"   Erros REFINADOS (com feedback): {len(df_output[df_output['origem'] == 'REFINADO_FEEDBACK'])}")
    print(f"   Erros MAPEADOS: {len(df_output[df_output['origem'] == 'MAPEADO'])}")
    print(f"   Erros INFERIDOS: {len(df_output[df_output['origem'] == 'INFERIDO'])}")
    print(f"   Respostas consolidadas disponíveis: {len(df_respostas_consolidadas)}")
    print(f"   Respostas refinadas disponíveis: {len(df_respostas_refinadas)}")
    print(f"\n📁 Arquivo gerado: {output_path}")
    print(f"📅 Data de execução: {data_geracao}")
    print(f"🏷️  Versão: {versao}")
    print(f"⏱️  Tempo de execução: {tempo_formatado} ({tempo_total:.2f} segundos)")
    print("\n💡 PRÓXIMO PASSO:")
    print("   Execute: python src/Teste_gio/2_ingest_to_bigquery.py")
    print("=" * 80)


if __name__ == "__main__":
    main()
