#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from google.api_core import exceptions
from google.cloud import storage
from datetime import datetime
import os
import csv
import time
import sys
import gc
from typing import Optional, Dict, Any, List
import xmltodict
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
# import subprocess
# subprocess.check_call(["pip", "install", "xmltodict==1.0.2"])
"""
Processamento de XMLs NFe em Dataproc Job
Arquivo: process_nfe_xmls.py
"""

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURAÇÃO
# ============================================================
# Obter ambiente desde argumentos ou variável de ambiente
if len(sys.argv) > 1:
    enviroment = sys.argv[1]
    print(f"📋 Ambiente desde argumentos: {enviroment}")
else:
    enviroment = 'DEV'
    print(f"📋 Ambiente desde variável: {enviroment}")

INPUT_BUCKET = "tributario_xml_file"
OUTPUT_BUCKET = "tributario_raw"

if enviroment == "PROD":
    INPUT_PREFIX = "202512/"
    OUTPUT_PREFIX = "xml"
else:
    INPUT_PREFIX = "input_test/"
    OUTPUT_PREFIX = "xml1"
OUTPUT_PATH = f"gs://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}"
BATCH_SIZE = 40000
print("="*60)
print("🚀 INICIANDO DATAPROC JOB - Processamento NFe XMLs")
print("="*60)
print(f"📥 Input:  gs://{INPUT_BUCKET}/{INPUT_PREFIX}")
print(f"📤 Output: {OUTPUT_PATH}")
print(f"🔢 Batch:  {BATCH_SIZE:,}")
print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

# ============================================================
# INICIALIZAR SPARK
# ============================================================
spark = SparkSession.builder \
    .appName("NFe_XML_Processor") \
    .master("yarn") \
    .config("spark.driver.memory", "6g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "3") \
    .config("spark.executor.memory", "11g") \
    .config("spark.executor.memoryOverhead", "1g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.files.openCostInBytes", "4194304") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .config("spark.hadoop.fs.gs.implicit.dir.repair.enable", "false") \
    .config("spark.hadoop.fs.gs.metadata.cache.enable", "true") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"✅ Spark inicializado - Version: {spark.version}")

# ============================================================
# FUNÇÕES DE PROCESSAMENTO
# ============================================================


def obter_modelo_pelo_nome_arquivo(file_path: str) -> Optional[str]:
    """
    Extrai o modelo do documento (dígitos 21 e 22) do nome do arquivo XML.
    Ex: '''31251233041260146371550000118837001120030036.xml''' -> "55"
    """
    try:
        filename = os.path.basename(file_path)
        # O modelo são os caracteres na posição 20 e 21 (0-indexed)
        modelo = filename[20:22]
        if modelo.isdigit() and len(modelo) == 2:
            return modelo
        else:
            logger.warning(
                f"Não foi possível extrair um modelo válido do nome do arquivo: {filename}")
            return None
    except IndexError:
        logger.error(
            f"Nome do arquivo muito curto para extrair o modelo: {filename}")
        return None


def achar_info_tags(node: Any) -> Optional[Dict]:
    """
    Tenta encontrar dinamicamente a tag de informações principal (que contém 'ide')
    percorrendo RECURSIVAMENTE o dicionário ou lista.
    """
    # Caso 1: O nó atual é um dicionário
    if isinstance(node, dict):
        # Heurística: Se a chave 'ide' existe, encontramos o nó correto.
        if 'ide' in node and isinstance(node['ide'], dict):
            return node

        # Se não, aplica a busca recursiva em cada valor dentro deste dicionário.
        for key, value in node.items():
            # Ignora atributos para eficiência
            if not key.startswith('@'):
                resultado = achar_info_tags(value)
                # Se um dos "clones" achou o tesouro, retorna o resultado imediatamente.
                if resultado is not None:
                    return resultado

    # Caso 2: O nó atual é uma lista
    elif isinstance(node, list):
        # Aplica a busca recursiva em cada item da lista.
        for item in node:
            resultado = achar_info_tags(item)
            # Se um dos "clones" achou o tesouro, retorna o resultado.
            if resultado is not None:
                return resultado

    # Se não for dicionário nem lista, ou se a busca em todos os caminhos falhou, retorna None.
    return None


def escribir_error(chave_acesso: str, erro: str):
    """Escreve erro no arquivo CSV no GCS"""
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob_name = f"errors/xml_errors_{enviroment}_{datetime.now().strftime('%Y%m%d')}.csv"
    blob = bucket.blob(blob_name)

    linha_error = f"{chave_acesso},{str(erro)}\n"

    if blob.exists():
        content = blob.download_as_text()
        content += linha_error
    else:
        content = "chave_acesso,erro\n" + linha_error

    blob.upload_from_string(content, content_type='text/csv')


def parse_xml_content(xml_content: str) -> Optional[Dict[str, Any]]:
    """Faz parse do XML para dicionário"""
    try:
        return xmltodict.parse(xml_content)
    except Exception:
        return None


def extrair_dados_nfe(xml_dict, tipo=str, blob_name: str = "unknown") -> Optional[List[Dict]]:
    """Extrai dados da NFe"""
    logger.info("******Iniciando extração de dados NFe")
    if not xml_dict:
        print('sem dicionario')
        return None

    try:
        infNFe = achar_info_tags(xml_dict)

        if not infNFe:
            print(
                f"ERRO: Tags de informações principal (infNFe/infCte) não encontrado para o tipo {tipo}.")
            return None

        print(f"Tags de informações encontrado para o tipo: {tipo}")

        dest = {}
        endereco_cliente = {}
        totais = {}
        rem = {}
        endereco_remetente = {}
        if tipo in ['55', '65']:
            dest = infNFe.get('dest', {})
            endereco_cliente = dest.get('enderDest', {})
            totais = infNFe.get('total', {}).get('ICMSTot', {})
        elif tipo == '57':
            dest = infNFe.get('dest', {})
            endereco_cliente = dest.get('enderDest', {})
            rem = infNFe.get('rem', {})
            endereco_remetente = rem.get('enderReme', {})
        elif tipo == '67':
            dest = infNFe.get('toma', {})
            endereco_cliente = dest.get('enderToma', {})
        else:
            # Assumimos o padrão mais comum (destinatário). As regras para extrair cliente/totais não são conhecidas.
            dest = infNFe.get('dest', {})
            endereco_cliente = dest.get('enderDest', {})

        # Dados gerais
        ide = infNFe.get('ide', {})
        emit = infNFe.get('emit', {})

        # Extrair NFref
        nfref = ide.get('NFref')
        ide_nfref = 'N/A'
        if nfref:
            if isinstance(nfref, dict):
                ide_nfref = nfref.get('refNFe', nfref.get('refNF', 'N/A'))
            elif isinstance(nfref, list) and len(nfref) > 0:
                ide_nfref = nfref[0].get(
                    'refNFe', nfref[0].get('refNF', 'N/A'))

        # Dados base
        base_data = {
            'chave_acesso': infNFe.get('@Id', 'N/A'),
            'numero_factura': ide.get('nNF', ide.get('nCT', 'N/A')),
            'ide_serie': ide.get('serie', 'N/A'),
            'ide_mod': ide.get('mod', 'N/A'),
            'ide_cuf': ide.get('cUF', 'N/A'),
            'ide_data_emissao': ide.get('dhEmi', 'N/A'),
            'ide_cmunicipio': ide.get('cMunFG', 'N/A'),
            'ide_nfref': ide_nfref,
            'emisor_cnpj': emit.get('CNPJ', 'N/A'),
            'emisor_ie': emit.get('IE', 'N/A'),
            'emisor_razao_social': emit.get('xNome', 'N/A'),
            'emisor_cmunicipio': emit.get('enderEmit', {}).get('cMun', 'N/A'),
            'emisor_uf': emit.get('enderEmit', {}).get('UF', 'N/A'),
            'emisor_fone': emit.get('enderEmit', {}).get('fone', 'N/A'),
            'emisor_cep': emit.get('enderEmit', {}).get('CEP', 'N/A'),
            'emisor_cPais': emit.get('enderEmit', {}).get('cPais', 'N/A'),
            'emisor_nro': emit.get('enderEmit', {}).get('nro', 'N/A'),
            'emisor_xBairro': emit.get('enderEmit', {}).get('xBairro', 'N/A'),
            'emisor_xLgr': emit.get('enderEmit', {}).get('xLgr', 'N/A'),
            'emisor_xMun': emit.get('enderEmit', {}).get('xMun', 'N/A'),
            'emisor_xFant': emit.get('enderEmit', {}).get('xFant', 'N/A'),
            'cliente_cnpj_cpf': dest.get('CNPJ', dest.get('CPF', 'N/A')),
            'cliente_nome': dest.get('xNome', 'N/A'),
            'cliente_ie': dest.get('IE', 'N/A'),
            'cliente_cmunicipio': endereco_cliente.get('cMun', 'N/A'),
            'cliente_uf': endereco_cliente.get('UF', 'N/A'),
            'cliente_cep': endereco_cliente.get('CEP', 'N/A'),
            'cliente_nro': endereco_cliente.get('nro', 'N/A'),
            'cliente_xBairro': endereco_cliente.get('xBairro', 'N/A'),
            'cliente_xCpl': endereco_cliente.get('xCpl', 'N/A'),
            'cliente_xLgr': endereco_cliente.get('xLgr', 'N/A'),
            'cliente_xMun': endereco_cliente.get('xMun', 'N/A'),
            'cliente_fone': endereco_cliente.get('fone', 'N/A'),
            'total_valor_produtos': float(totais.get('vProd', 0)),
            'total_desconto': float(totais.get('vDesc', 0)),
            'total_VNF': float(totais.get('vNF', 0)),
            'total_vFCP': float(totais.get('vFCP', 0)),
            'total_vIPI': float(totais.get('vIPI', 0)),
            'total_vbc': float(totais.get('vBC', 0)),
            'total_vST': float(totais.get('vST', 0)),
            'total_vFCPST': float(totais.get('vFCPST', 0)),
            'total_vFCPSTRet': float(totais.get('vFCPSTRet', 0)),
            # generado por IA
            'ide_cDV': ide.get('cDV', 'N/A'),
            'ide_dhSaiEnti': ide.get('dhSaiEnti', 'N/A'),
            'ide_indFinal': ide.get('indFinal', 'N/A'),
            'ide_tpEmis': ide.get('tpEmis', 'N/A'),
            'ide_tpNF': ide.get('tpNF', 'N/A'),
            'total_vBCST': float(totais.get('vBCST', 0)),
            'total_vCOFINS': float(totais.get('vCOFINS', 0)),
            'total_vFrete': float(totais.get('vFrete', 0)),
            'total_vICMS': float(totais.get('vICMS', 0)),
            'total_vICMSDeson': float(totais.get('vICMSDeson', 0)),
            'total_vOutro': float(totais.get('vOutro', 0)),
            'total_vPIS': float(totais.get('vPIS', 0)),
            'total_vSeg': float(totais.get('vSeg', 0)),
            # Campos CTe (modelo 57)
            'CTe_ide_CFOP': ide.get('CFOP', 'N/A'),
            'CTe_ide_cDV': ide.get('cDV', 'N/A'),
            'CTe_infCte': infNFe.get('@Id', 'N/A'),
            'CTe_rem_CNPJ': rem.get('CNPJ', 'N/A'),
            'CTe_rem_IE': rem.get('IE', 'N/A'),
            'CTe_rem_xNome': rem.get('xNome', 'N/A'),
            'CTe_rem_xFant': rem.get('xFant', 'N/A'),
            'CTe_rem_fone': rem.get('fone', 'N/A'),
            'CTe_rem_email': rem.get('email', 'N/A'),
            'CTe_rem_enderReme_xLgr': endereco_remetente.get('xLgr', 'N/A'),
            'CTe_rem_enderReme_nro': endereco_remetente.get('nro', 'N/A'),
            'CTe_rem_enderReme_xBairro': endereco_remetente.get('xBairro', 'N/A'),
            'CTe_rem_enderReme_cMun': endereco_remetente.get('cMun', 'N/A'),
            'CTe_rem_enderReme_xMun': endereco_remetente.get('xMun', 'N/A'),
            'CTe_rem_enderReme_CEP': endereco_remetente.get('CEP', 'N/A'),
            'CTe_rem_enderReme_UF': endereco_remetente.get('UF', 'N/A'),
            'CTe_rem_enderReme_cPais': endereco_remetente.get('cPais', 'N/A'),
            'CTe_dest_CPF': dest.get('CPF', 'N/A'),
            'CTe_dest_xNome': dest.get('xNome', 'N/A'),
            'CTe_dest_email': dest.get('email', 'N/A'),
            'CTe_dest_enderDest_xLgr': endereco_cliente.get('xLgr', 'N/A'),
            'CTe_dest_enderDest_nro': endereco_cliente.get('nro', 'N/A'),
            'CTe_dest_enderDest_xBairro': endereco_cliente.get('xBairro', 'N/A'),
            'CTe_dest_enderDest_cMun': endereco_cliente.get('cMun', 'N/A'),
            'CTe_dest_enderDest_xMun': endereco_cliente.get('xMun', 'N/A'),
            'CTe_dest_enderDest_CEP': endereco_cliente.get('CEP', 'N/A'),
            'CTe_dest_enderDest_UF': endereco_cliente.get('UF', 'N/A'),
            'CTe_dest_enderDest_cPais': endereco_cliente.get('cPais', 'N/A'),
        }
        # print(base_data)
        # Produtos
        seccion_produtos = infNFe.get('det')
        if not seccion_produtos:
            empty_row = base_data.copy()
            empty_row.update({
                'produto_sku': 'N/A',
                'produto_nro_item': 'N/A',
                'produto_nome': 'N/A',
                'produto_cfop': 'N/A',
                'produto_cest': 'N/A',
                'produto_ean': 'N/A',
                'produto_frete': 0.0,
                'produto_VSeg': 0.0,
                'produto_ncm': 'N/A',
                'produto_icms_cst': 'N/A',
                'produto_icms_vbc': 0.0,
                'produto_icms_pICMS': 0.0,
                'produto_icms_vICMS': 0.0,
                'produto_icms_vFCP': 0.0,
                'produto_pis_cst': 'N/A',
                'produto_pis_vbc': 0.0,
                'produto_pis_pPIS': 0.0,
                'produto_pis_vPIS': 0.0,
                'produto_cofins_cst': 'N/A',
                'produto_cofins_vbc': 0.0,
                'produto_cofins_pCOFINS': 0.0,
                'produto_cofins_vCOFINS': 0.0,
                'produto_ipi_cst': 'N/A',
                'produto_ipi_vbc': 0.0,
                'produto_ipi_pIPI': 0.0,
                'produto_ipi_vIPI': 0.0,
                'produto_difal_vFCPUFDest': 0.0,
                'produto_difal_vICMSUFDest': 0.0,
                'produto_cantidad': 0.0,
                'produto_unidad': 'N/A',
                'produto_valor_unitario': 0.0,
                'produto_valor_total': 0.0,
                'produto_desconto': 0.0,
                # generado por IA
                'produto_cBenef': 'N/A',
                'produto_cEANTrib': 'N/A',
                'produto_qTrib': 0.0,
                'produto_uTrib': 'N/A',
                'produto_vUnTrib': 0.0,
                'produto_difal_pFCPUFDest': 0.0,
                'produto_difal_pICMSInter': 0.0,
                'produto_difal_pICMSUFDest': 0.0,
                'produto_difal_vICMSUFRemet': 0.0,
                'produto_icms_CSOSN': 'N/A',
                'produto_icms_orig': 'N/A',
                'produto_icms_pICMSEfet': 0.0,
                'produto_icms_pICMSST': 0.0,
                'produto_icms_pRedBC': 0.0,
                'produto_icms_pST': 0.0,
                'produto_icms_vBCEfet': 0.0,
                'produto_icms_vBCFCPSTRet': 0.0,
                'produto_icms_vBCST': 0.0,
                'produto_icms_vBCSTRet': 0.0,
                'produto_icms_vICMSDeson': 0.0,
                'produto_icms_vICMSEfet': 0.0,
                'produto_icms_vICMSST': 0.0,
                'produto_icms_vICMSSTRet': 0.0,
                'produto_icms_vICMSSubstituto': 0.0,
                'produto_pis_PISST': 0.0,
                'produto_pis_PISQtde': 'N/A'
            })
            return [empty_row]

        lista_produtos = seccion_produtos if isinstance(
            seccion_produtos, list) else [seccion_produtos]

        resultados = []
        for item in lista_produtos:
            prod = item.get('prod', {})
            imposto = item.get('imposto', {})
            numero_item = item.get('@nItem', 'N/A')
            # Extrair ICMS
            icms_top_level = imposto.get('ICMS', {})
            # 1. Pega o nome da tag (ex: ICMS00)
            dados_icms = list(icms_top_level.values())[
                0] if icms_top_level else {}
            icms_cst_tipo = dados_icms.get(
                'CST', dados_icms.get('CSOSN', 'N/A'))
            icms = mapear_cst(icms_cst_tipo, dados_icms)

            # Extrair PIS
            pis_top_level = imposto.get('PIS', {})
            # 1. Pega o nome da tag (ex: PISAliq)
            dados_pis = list(pis_top_level.values())[
                0] if pis_top_level else {}
            pis_cst_tipo = dados_pis.get('CST', 'N/A')
            pis = mapear_pis(pis_cst_tipo, dados_pis)
            # Extrair COFINS
            cofins_top_level = imposto.get('COFINS', {})
            # 1. Pega o nome da tag (ex: COFINSAliq)
            dados_cofins = list(cofins_top_level.values())[
                0] if cofins_top_level else {}
            cofins_cst_tipo = dados_cofins.get('CST', 'N/A')
            confins = mapear_cofins(cofins_cst_tipo, dados_cofins)
            # Extrair IPI
            ipi_top_level = imposto.get('IPI')
            ipi_dados = {}
            if isinstance(ipi_top_level, dict):
                # Encontra a tag interna (pode ser 'IPITrib' ou 'IPINT')
                # e ignora outras tags como 'cEnq'
                chave_interna_ipi = next(
                    (key for key in ipi_top_level if key.startswith('IPI')), None)
                if chave_interna_ipi:
                    ipi_dados = ipi_top_level[chave_interna_ipi]

            ipi_cst_tipo = ipi_dados.get('CST', 'N/A')
            # Chama a função mapear_ipi correta, passando os dados do IPI
            ipi = mapear_ipi(ipi_cst_tipo, ipi_dados)
            # Extrair ICMSUFDest (DIFAL)
            icms_uf_dest_top_level = imposto.get('ICMSUFDest', 'N/A')
            if icms_uf_dest_top_level != 'N/A':
                difal = mapear_icms_uf_dest(icms_uf_dest_top_level)
            else:
                difal = {
                    'vFCPUFDest': 0.0,
                    'vICMSUFDest': 0.0,
                }

            row = base_data.copy()
            row.update({
                'produto_sku': prod.get('cProd', 'N/A'),
                'produto_nro_item': numero_item,  # numero_item
                'produto_nome': prod.get('xProd', 'N/A'),
                'produto_cfop': prod.get('CFOP', 'N/A'),
                'produto_cest': prod.get('CEST', 'N/A'),
                'produto_ean': prod.get('cEAN', 'N/A'),
                'produto_frete': prod.get('vFrete', 0),
                'produto_VSeg': prod.get('vSeg', 0),
                'produto_ncm': prod.get('NCM', 'N/A'),
                # adicionamos os campos de imposto ICMS
                'produto_icms_cst': icms_cst_tipo,
                'produto_icms_vbc': icms["vBC"],
                'produto_icms_pICMS': icms["pICMS"],
                'produto_icms_vICMS': icms["vICMS"],
                'produto_icms_vFCP': float(dados_icms.get("vFCP", 0)),
                # adicionamos os campos de imposto pis
                'produto_pis_cst': pis_cst_tipo,
                'produto_pis_vbc': pis["vBC"],
                'produto_pis_pPIS': pis["pPIS"],
                'produto_pis_vPIS': pis["vPIS"],
                # adicionamos os campos de imposto COFINS
                'produto_cofins_cst': cofins_cst_tipo,
                'produto_cofins_vbc': confins["vBC"],
                'produto_cofins_pCOFINS': confins["pCOFINS"],
                'produto_cofins_vCOFINS': confins["vCOFINS"],
                # adicionamos os campos de imposto IPI
                'produto_ipi_cst': ipi_cst_tipo,
                'produto_ipi_vbc': ipi["vBC"],
                'produto_ipi_pIPI': ipi["pIPI"],
                'produto_ipi_vIPI': ipi["vIPI"],
                # adicionamos os campos de ICMSUFDest
                'produto_difal_vFCPUFDest': difal['vFCPUFDest'],
                'produto_difal_vICMSUFDest': difal['vICMSUFDest'],
                'produto_cantidad': float(prod.get('qCom', 0)),
                'produto_unidad': prod.get('uCom', 'N/A'),
                'produto_valor_unitario': float(prod.get('vUnCom', 0)),
                'produto_valor_total': float(prod.get('vProd', 0)),
                'produto_desconto': float(prod.get('vDesc', 0)),
                # generado por IA
                'produto_cBenef': prod.get('cBenef', 'N/A'),
                'produto_cEANTrib': prod.get('cEANTrib', 'N/A'),
                'produto_qTrib': float(prod.get('qTrib', 0)),
                'produto_uTrib': prod.get('uTrib', 'N/A'),
                'produto_vUnTrib': float(prod.get('vUnTrib', 0)),
                'produto_difal_pFCPUFDest': float(icms_uf_dest_top_level.get('pFCPUFDest', 0)) if isinstance(icms_uf_dest_top_level, dict) else 0.0,
                'produto_difal_pICMSInter': float(icms_uf_dest_top_level.get('pICMSInter', 0)) if isinstance(icms_uf_dest_top_level, dict) else 0.0,
                'produto_difal_pICMSUFDest': float(icms_uf_dest_top_level.get('pICMSUFDest', 0)) if isinstance(icms_uf_dest_top_level, dict) else 0.0,
                'produto_difal_vICMSUFRemet': float(icms_uf_dest_top_level.get('vICMSUFRemet', 0)) if isinstance(icms_uf_dest_top_level, dict) else 0.0,
                'produto_icms_CSOSN': dados_icms.get('CSOSN', 'N/A'),
                'produto_icms_orig': dados_icms.get('orig', 'N/A'),
                'produto_icms_pICMSEfet': float(dados_icms.get('pICMSEfet', 0)),
                'produto_icms_pICMSST': float(dados_icms.get('pICMSST', 0)),
                'produto_icms_pRedBC': float(dados_icms.get('pRedBC', 0)),
                'produto_icms_pST': float(dados_icms.get('pST', 0)),
                'produto_icms_vBCEfet': float(dados_icms.get('vBCEfet', 0)),
                'produto_icms_vBCFCPSTRet': float(dados_icms.get('vBCFCPSTRet', 0)),
                'produto_icms_vBCST': float(dados_icms.get('vBCST', 0)),
                'produto_icms_vBCSTRet': float(dados_icms.get('vBCSTRet', 0)),
                'produto_icms_vICMSDeson': float(dados_icms.get('vICMSDeson', 0)),
                'produto_icms_vICMSEfet': float(dados_icms.get('vICMSEfet', 0)),
                'produto_icms_vICMSST': float(dados_icms.get('vICMSST', 0)),
                'produto_icms_vICMSSTRet': float(dados_icms.get('vICMSSTRet', 0)),
                'produto_icms_vICMSSubstituto': float(dados_icms.get('vICMSSubstituto', 0)),
                'produto_pis_PISST': float(pis_top_level.get('PISST', {}).get('vPIS', 0)),
                'produto_pis_PISQtde': dados_pis.get('PISQtde', 'N/A')

            })
            resultados.append(row)
            logger.info(
                f"*****Produto processado - Total items: {len(resultados)}")

        return resultados

    except Exception as e:
        chave_acesso = blob_name.split(".")[0]
        escribir_error(chave_acesso, str(e))
        return None


def process_xml_from_gcs(bucket_name: str, blob_name: str) -> Optional[List[Dict]]:
    """Lê e processa um XML desde GCS"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        logger.info(f"******Processando XML: {blob_name}")
        xml_content = blob.download_as_text(encoding='utf-8')
        xml_dict = parse_xml_content(xml_content)
        tipo = obter_modelo_pelo_nome_arquivo(blob_name)
        rows = extrair_dados_nfe(xml_dict, tipo)

        del xml_content, xml_dict
        return rows if rows else []
    except Exception:
        return []

# ============================================================
# SCHEMA
# ============================================================


schema = StructType([
    StructField("chave_acesso", StringType(), True),
    StructField("numero_factura", StringType(), True),
    StructField("ide_serie", StringType(), True),
    StructField("ide_mod", StringType(), True),
    StructField("ide_cuf", StringType(), True),
    StructField("ide_data_emissao", StringType(), True),
    StructField("ide_cmunicipio", StringType(), True),
    StructField("ide_nfref", StringType(), True),
    StructField("emisor_cnpj", StringType(), True),
    StructField("emisor_ie", StringType(), True),
    StructField("emisor_razao_social", StringType(), True),
    StructField("emisor_cmunicipio", StringType(), True),
    StructField("emisor_uf", StringType(), True),
    StructField("emisor_fone", StringType(), True),
    StructField("emisor_cep", StringType(), True),
    StructField("emisor_cPais", StringType(), True),
    StructField("emisor_nro", StringType(), True),
    StructField("emisor_xBairro", StringType(), True),
    StructField("emisor_xLgr", StringType(), True),
    StructField("emisor_xMun", StringType(), True),
    StructField("emisor_xFant", StringType(), True),
    StructField("cliente_cnpj_cpf", StringType(), True),
    StructField("cliente_nome", StringType(), True),
    StructField("cliente_ie", StringType(), True),
    StructField("cliente_cmunicipio", StringType(), True),
    StructField("cliente_uf", StringType(), True),
    StructField("cliente_cep", StringType(), True),
    StructField("cliente_nro", StringType(), True),
    StructField("cliente_xBairro", StringType(), True),
    StructField("cliente_xCpl", StringType(), True),
    StructField("cliente_xLgr", StringType(), True),
    StructField("cliente_xMun", StringType(), True),
    StructField("cliente_fone", StringType(), True),
    StructField("total_valor_produtos", DoubleType(), True),
    StructField("total_desconto", DoubleType(), True),
    StructField("total_VNF", DoubleType(), True),
    StructField("total_vFCP", DoubleType(), True),
    StructField("total_vIPI", DoubleType(), True),
    StructField("total_vbc", DoubleType(), True),
    StructField("total_vST", DoubleType(), True),
    StructField("total_vFCPST", DoubleType(), True),
    StructField("total_vFCPSTRet", DoubleType(), True),
    # generado por IA
    StructField("ide_cDV", StringType(), True),
    StructField("ide_dhSaiEnti", StringType(), True),
    StructField("ide_indFinal", StringType(), True),
    StructField("ide_tpEmis", StringType(), True),
    StructField("ide_tpNF", StringType(), True),
    StructField("total_vBCST", DoubleType(), True),
    StructField("total_vCOFINS", DoubleType(), True),
    StructField("total_vFrete", DoubleType(), True),
    StructField("total_vICMS", DoubleType(), True),
    StructField("total_vICMSDeson", DoubleType(), True),
    StructField("total_vOutro", DoubleType(), True),
    StructField("total_vPIS", DoubleType(), True),
    StructField("total_vSeg", DoubleType(), True),
    # Campos CTe (modelo 57)
    StructField("CTe_ide_CFOP", StringType(), True),
    StructField("CTe_ide_cDV", StringType(), True),
    StructField("CTe_infCte", StringType(), True),
    StructField("CTe_rem_CNPJ", StringType(), True),
    StructField("CTe_rem_IE", StringType(), True),
    StructField("CTe_rem_xNome", StringType(), True),
    StructField("CTe_rem_xFant", StringType(), True),
    StructField("CTe_rem_fone", StringType(), True),
    StructField("CTe_rem_email", StringType(), True),
    StructField("CTe_rem_enderReme_xLgr", StringType(), True),
    StructField("CTe_rem_enderReme_nro", StringType(), True),
    StructField("CTe_rem_enderReme_xBairro", StringType(), True),
    StructField("CTe_rem_enderReme_cMun", StringType(), True),
    StructField("CTe_rem_enderReme_xMun", StringType(), True),
    StructField("CTe_rem_enderReme_CEP", StringType(), True),
    StructField("CTe_rem_enderReme_UF", StringType(), True),
    StructField("CTe_rem_enderReme_cPais", StringType(), True),
    StructField("CTe_dest_CPF", StringType(), True),
    StructField("CTe_dest_xNome", StringType(), True),
    StructField("CTe_dest_email", StringType(), True),
    StructField("CTe_dest_enderDest_xLgr", StringType(), True),
    StructField("CTe_dest_enderDest_nro", StringType(), True),
    StructField("CTe_dest_enderDest_xBairro", StringType(), True),
    StructField("CTe_dest_enderDest_cMun", StringType(), True),
    StructField("CTe_dest_enderDest_xMun", StringType(), True),
    StructField("CTe_dest_enderDest_CEP", StringType(), True),
    StructField("CTe_dest_enderDest_UF", StringType(), True),
    StructField("CTe_dest_enderDest_cPais", StringType(), True),
    StructField("produto_sku", StringType(), True),
    StructField("produto_nro_item", StringType(), True),
    StructField("produto_nome", StringType(), True),
    # StructField("produton_cm", StringType(), True),
    StructField("produto_cfop", StringType(), True),
    StructField("produto_cest", StringType(), True),
    StructField("produto_ean", StringType(), True),
    StructField("produto_frete", StringType(), True),
    StructField("produto_VSeg", StringType(), True),
    StructField("produto_ncm", StringType(), True),
    # adicionamos os campos de imposto ICMS
    StructField("produto_icms_cst", StringType(), True),
    StructField("produto_icms_vbc", DoubleType(), True),
    StructField("produto_icms_pICMS", DoubleType(), True),
    StructField("produto_icms_vICMS", DoubleType(), True),
    StructField("produto_icms_vFCP", DoubleType(), True),
    # adicionamos os campos de imposto pis
    StructField("produto_pis_cst", StringType(), True),
    StructField("produto_pis_vbc", DoubleType(), True),
    StructField("produto_pis_pPIS", DoubleType(), True),
    StructField("produto_pis_vPIS", DoubleType(), True),
    # adicionamos os campos de imposto COFINS
    StructField("produto_cofins_cst", StringType(), True),
    StructField("produto_cofins_vbc", DoubleType(), True),
    StructField("produto_cofins_pCOFINS", DoubleType(), True),
    StructField("produto_cofins_vCOFINS", DoubleType(), True),
    # adicionamos os campos de imposto IPI
    StructField("produto_ipi_cst", StringType(), True),
    StructField("produto_ipi_vbc", DoubleType(), True),
    StructField("produto_ipi_pIPI", DoubleType(), True),
    StructField("produto_ipi_vIPI", DoubleType(), True),
    StructField("produto_difal_vFCPUFDest", DoubleType(), True),
    StructField("produto_difal_vICMSUFDest", DoubleType(), True),
    StructField("produto_cantidad", DoubleType(), True),
    StructField("produto_unidad", StringType(), True),
    StructField("produto_valor_unitario", DoubleType(), True),
    StructField("produto_valor_total", DoubleType(), True),
    StructField("produto_desconto", DoubleType(), True),
    # generado por IA
    StructField("produto_cBenef", StringType(), True),
    StructField("produto_cEANTrib", StringType(), True),
    StructField("produto_qTrib", DoubleType(), True),
    StructField("produto_uTrib", StringType(), True),
    StructField("produto_vUnTrib", DoubleType(), True),
    StructField("produto_difal_pFCPUFDest", DoubleType(), True),
    StructField("produto_difal_pICMSInter", DoubleType(), True),
    StructField("produto_difal_pICMSUFDest", DoubleType(), True),
    StructField("produto_difal_vICMSUFRemet", DoubleType(), True),
    StructField("produto_icms_CSOSN", StringType(), True),
    StructField("produto_icms_orig", StringType(), True),
    StructField("produto_icms_pICMSEfet", DoubleType(), True),
    StructField("produto_icms_pICMSST", DoubleType(), True),
    StructField("produto_icms_pRedBC", DoubleType(), True),
    StructField("produto_icms_pST", DoubleType(), True),
    StructField("produto_icms_vBCEfet", DoubleType(), True),
    StructField("produto_icms_vBCFCPSTRet", DoubleType(), True),
    StructField("produto_icms_vBCST", DoubleType(), True),
    StructField("produto_icms_vBCSTRet", DoubleType(), True),
    StructField("produto_icms_vICMSDeson", DoubleType(), True),
    StructField("produto_icms_vICMSEfet", DoubleType(), True),
    StructField("produto_icms_vICMSST", DoubleType(), True),
    StructField("produto_icms_vICMSSTRet", DoubleType(), True),
    StructField("produto_icms_vICMSSubstituto", DoubleType(), True),
    StructField("produto_pis_PISST", DoubleType(), True),
    StructField("produto_pis_PISQtde", StringType(), True),
])

# ============================================================
# LISTAR ARQUIVOS XML
# ============================================================


def list_xml_files_in_gcs(bucket_name: str, prefix: str = "") -> List[tuple]:
    """Lista arquivos XML no GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    xml_files = [(bucket_name, blob.name)
                 for blob in blobs
                 if blob.name.lower().endswith('.xml')]
    return xml_files


print("\n📂 Listando arquivos XML no GCS...")
xml_files = list_xml_files_in_gcs(INPUT_BUCKET, INPUT_PREFIX)
total_files = len(xml_files)
print(f"✅ Encontrados {total_files:,} arquivos XML")
if total_files == 0:
    print("❌ Não foram encontrados arquivos XML. Encerrando.")
    sys.exit(1)
# ============================================================
# FUNÇÃO DE PROCESSAMENTO POR LOTES
# ============================================================


def mapear_cst(cst_entrada: str, dados: Dict) -> Dict[str, float]:
    """
    Função base para normalizar valores.
    Correções aplicadas:
    1. float('vBC') -> float(dados.get('vBC', 0)) para evitar erros.
    2. Unificação: Mapeia os valores de 'ret' (CST 60) para as variáveis principais
       (vBC) para manter a consistência no BigQuery.
    """
    # Inicializamos APENAS as variáveis que irão para o BigQuery
    resultado = {
        'vBC': 0.0,
        'pICMS': 0.0,
        'vICMS': 0.0,
    }

    # Helper para converter para float com segurança
    def obter_float(chave):
        return float(dados.get(chave, 0))

    if 'vBC' in dados:
        resultado['vBC'] = obter_float('vBC')
        resultado['pICMS'] = obter_float('pICMS')
        resultado['vICMS'] = obter_float('vICMS')
    return resultado


def mapear_pis(cst_entrada: str, dados: Dict) -> Dict[str, float]:
    """
    Função para normalizar valores de PIS.
    Tags comuns: PISAliq (01, 02), PISQtde (03), PISNT (04-09), PISOutr (99).
    """
    resultado = {
        'vBC': 0.0,
        'pPIS': 0.0,
        'vPIS': 0.0,
    }

    def obter_float(chave):
        return float(dados.get(chave, 0))
    # CST 01, 02 e 99 (Outros - quando tem base de cálculo monetária)
    if 'vBC' in dados:
        resultado['vBC'] = obter_float('vBC')
        resultado['pPIS'] = obter_float('pPIS')
        resultado['vPIS'] = obter_float('vPIS')
    return resultado


def mapear_cofins(cst_entrada: str, dados: Dict) -> Dict[str, float]:
    """
    Função para normalizar valores de COFINS.
    Tags comuns: COFINSAliq (01, 02), COFINSQtde (03), COFINSNT (04-09), COFINSOutr (99).
    """
    resultado = {
        'vBC': 0.0,
        'pCOFINS': 0.0,
        'vCOFINS': 0.0,
    }

    def obter_float(chave):
        return float(dados.get(chave, 0))
    if 'vBC' in dados:
        resultado['vBC'] = obter_float('vBC')
        resultado['pCOFINS'] = obter_float('pCOFINS')
        resultado['vCOFINS'] = obter_float('vCOFINS')

    return resultado


def mapear_ipi(cst_entrada: str, dados: Dict) -> Dict[str, float]:
    def obter_float(chave):
        return float(dados.get(chave, 0))
    resultado = {
        'vBC': obter_float('vBC'),
        'pIPI': obter_float('pIPI'),
        'vIPI': obter_float('vIPI'),
    }
    return resultado


def mapear_icms_uf_dest(dados: Dict) -> Dict[str, float]:
    """
    Função para normalizar valores de ICMSUFDest (DIFAL).
    """
    resultado = {
        'vFCPUFDest': 0.0,
        'vICMSUFDest': 0.0,
    }

    def obter_float(chave):
        return float(dados.get(chave, 0))
    if 'vFCPUFDest' in dados:
        resultado['vFCPUFDest'] = obter_float('vFCPUFDest')
    if 'vICMSUFDest' in dados:
        resultado['vICMSUFDest'] = obter_float('vICMSUFDest')
    return resultado


def process_batch(batch_files: List[tuple], batch_num: int, total_batches: int):
    """Processa um lote de arquivos"""
    print(f"\n{'='*60}")
    print(f"🔄 Lote {batch_num}/{total_batches} - {len(batch_files):,} arquivos")
    print(f"{'='*60}")

    start_time = time.time()

    batch_size = len(batch_files)
    num_slices = batch_size if batch_size < 100 else 100
    try:
        batch_rdd = sc.parallelize(batch_files, numSlices=num_slices)

        def process_file_tuple(file_tuple):
            try:
                bucket_name, blob_name = file_tuple
                rows = process_xml_from_gcs(bucket_name, blob_name)
                return rows if rows else []
            except Exception:
                return []
        processed_rdd = batch_rdd.flatMap(process_file_tuple)
        processed_rdd.cache()
        df_batch = spark.createDataFrame(processed_rdd, schema=schema)
        df_batch = df_batch.withColumn(
            "data_emissao",
            coalesce(
                date_format(
                    to_date(
                        regexp_replace(col("ide_data_emissao"), "T.*", ""),
                        "yyyy-MM-dd"
                    ),
                    "yyyy-MM-dd"
                ),
                lit("data_invalida")
            )
        )
        count = df_batch.count()
        if count > 0:
            df_batch.write \
                .mode("append") \
                .partitionBy("data_emissao") \
                .parquet(OUTPUT_PATH)
        elapsed = time.time() - start_time
        print(f"✅ Lote {batch_num} completado:")
        print(f"   📊 Registros: {count:,}")
        print(f"   ⏱️  Tempo: {elapsed:.2f}s")
        if count > 0:
            print(f"   ⚡ Velocidade: {count/elapsed:.2f} reg/s")
        processed_rdd.unpersist()
        df_batch.unpersist()
        del df_batch, processed_rdd, batch_rdd
        gc.collect()
        return count
    except Exception as e:
        print(f"❌ Erro no lote {batch_num}: {e}")
        import traceback
        traceback.print_exc()
        return 0
# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================


def chunks(lst, n):
    """Divide lista em chunks"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


batches = list(chunks(xml_files, BATCH_SIZE))
total_batches = len(batches)

print(f"\n{'='*60}")
print("🚀 INICIANDO PROCESSAMENTO")
print(f"{'='*60}")
print(f"📊 Total arquivos: {total_files:,}")
print(f"🔢 Total lotes: {total_batches}")
print(f"{'='*60}\n")

total_records = 0
successful_batches = 0
failed_batches = []
start_total = time.time()

for i, batch in enumerate(batches, 1):
    try:
        records = process_batch(batch, i, total_batches)

        if records > 0:
            total_records += records
            successful_batches += 1
        else:
            failed_batches.append(i)

        # Progresso
        progress = (i / total_batches) * 100
        elapsed = time.time() - start_total
        avg_time = elapsed / i
        eta = (total_batches - i) * avg_time

        print(f"\n📈 PROGRESO: {progress:.1f}% ({i}/{total_batches})")
        print(f"   📊 Registros: {total_records:,}")
        print(f"   ⏱️  Transcorrido: {elapsed/60:.1f} min")
        print(f"   ⏰ ETA: {eta/60:.1f} min")

        if i % 10 == 0:
            gc.collect()

    except Exception as e:
        print(f"❌ ERRO CRÍTICO lote {i}: {e}")
        failed_batches.append(i)
        continue

# ============================================================
# RESUMO FINAL
# ============================================================

elapsed_total = time.time() - start_total

print(f"\n{'='*60}")
print("🎉 PROCESSAMENTO COMPLETADO")
print(f"{'='*60}")
print(f"⏰ Tempo total: {elapsed_total/3600:.2f} horas")
print(f"📊 Total registros: {total_records:,}")
print(f"✅ Lotes bem-sucedidos: {successful_batches}/{total_batches}")
print(f"📈 Taxa de sucesso: {(successful_batches/total_batches)*100:.1f}%")

if failed_batches:
    print(f"⚠️  Lotes falhados: {len(failed_batches)}")

print(f"📂 Dados em: {OUTPUT_PATH}")
print(f"{'='*60}")

spark.stop()
sys.exit(0 if len(failed_batches) == 0 else 1)
