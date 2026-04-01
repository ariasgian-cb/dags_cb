from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig
import argparse

# 1. LER OS ARGUMENTOS PASSADOS PELO DAG DO AIRFLOW
# ==================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--entorno")
parser.add_argument("--project_id")
parser.add_argument("--gcs_name")
parser.add_argument("--gcs_name_parquet")
args = parser.parse_args()

enviroment = args.entorno
PROJECT_ID  = args.project_id
DATASET_ID  = 'RAW'
GCS_NAME    = args.gcs_name_parquet
if enviroment=="DEV":
    TABLE_ID = "test_arquivos_xml"
    preffix='xml1'
else:
    TABLE_ID = "arquivos_xml"
    preffix='xml'
GCS_URI = f"gs://{GCS_NAME}/{preffix}/"
    
    


# Inicializar cliente de BigQuery
# creds=service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
client_bq = bigquery.Client()

# Criar referência da tabela
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Configurar tabela externa
external_config = ExternalConfig("PARQUET")
external_config.source_uris = [f"{GCS_URI}*"]  
external_config.autodetect = False  # Usaremos esquema manual

# Configurar particionamento Hive
hive_partitioning = bigquery.HivePartitioningOptions()
hive_partitioning.mode = "AUTO"
hive_partitioning.source_uri_prefix = f"gs://{GCS_NAME}/{preffix}/"
hive_partitioning.require_partition_filter = False  # Mudar para True se quiser forçar filtros
external_config.hive_partitioning = hive_partitioning

# Definir o esquema baseado no seu StructType do Spark
schema = [
    bigquery.SchemaField("chave_acesso", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("numero_factura", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_serie", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_mod", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_cuf", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_data_emissao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_cmunicipio", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_nfref", "STRING", mode="NULLABLE",description="Este campo só existe se houver uma devolução do produto; este é o link para a chave de acesso original."),
    bigquery.SchemaField("emisor_cnpj", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_ie", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_razao_social", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_cmunicipio", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_uf", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_fone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_cep", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_cPais", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_nro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_xBairro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_xLgr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_xMun", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("emisor_xFant", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_cnpj_cpf", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_nome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_ie", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_cmunicipio", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_uf", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_cep", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_nro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_xBairro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_xCpl", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_xLgr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_xMun", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cliente_fone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("total_valor_produtos", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_desconto", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_VNF", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vFCP", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vIPI", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vbc", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vFCPST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vFCPSTRet", "FLOAT64", mode="NULLABLE"),
    # generado por IA
    bigquery.SchemaField("ide_cDV", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_dhSaiEnti", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_indFinal", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_tpEmis", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ide_tpNF", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("total_vBCST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vCOFINS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vFrete", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vICMS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vICMSDeson", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vOutro", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vPIS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_vSeg", "FLOAT64", mode="NULLABLE"),
    # Campos CTe (modelo 57)
    bigquery.SchemaField("CTe_ide_CFOP", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_ide_cDV", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_infCte", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_CNPJ", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_IE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_xNome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_xFant", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_fone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_xLgr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_nro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_xBairro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_cMun", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_xMun", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_CEP", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_UF", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_rem_enderReme_cPais", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_CPF", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_xNome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_xLgr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_nro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_xBairro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_cMun", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_xMun", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_CEP", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_UF", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CTe_dest_enderDest_cPais", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_sku", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_nro_item", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_nome", "STRING", mode="NULLABLE"),
    # bigquery.SchemaField("produton_cm", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_cfop", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_cest", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_ean", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_frete", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_VSeg", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("produto_ncm", "STRING", mode="NULLABLE"),        
    # adicionamos os campos de imposto ICMS
    bigquery.SchemaField("produto_icms_cst", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vbc", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_pICMS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vICMS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vFCP", "FLOAT64", mode="NULLABLE"),
    # adicionamos os campos de imposto PIS
    bigquery.SchemaField("produto_pis_cst", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_pis_vbc", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_pis_pPIS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_pis_vPIS", "FLOAT64", mode="NULLABLE"),
    # adicionamos os campos de imposto COFINS
    bigquery.SchemaField("produto_cofins_cst", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_cofins_vbc", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_cofins_pCOFINS", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_cofins_vCOFINS", "FLOAT64", mode="NULLABLE"),
    # adicionamos os campos de imposto IPI
    bigquery.SchemaField("produto_ipi_cst", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_ipi_vbc", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_ipi_pIPI", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_ipi_vIPI", "FLOAT64", mode="NULLABLE"),
    # Campos DIFAL
    bigquery.SchemaField("produto_difal_vFCPUFDest", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_difal_vICMSUFDest", "FLOAT64", mode="NULLABLE"),
    
    bigquery.SchemaField("produto_cantidad", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_unidad", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_valor_unitario", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_valor_total", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_desconto", "FLOAT64", mode="NULLABLE"),
    # generado por IA
    bigquery.SchemaField("produto_cBenef", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_cEANTrib", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_qTrib", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_uTrib", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_vUnTrib", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_difal_pFCPUFDest", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_difal_pICMSInter", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_difal_pICMSUFDest", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_difal_vICMSUFRemet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_CSOSN", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_orig", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_pICMSEfet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_pICMSST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_pRedBC", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_pST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vBCEfet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vBCFCPSTRet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vBCST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vBCSTRet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vICMSDeson", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vICMSEfet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vICMSST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vICMSSTRet", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_icms_vICMSSubstituto", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_pis_PISST", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("produto_pis_PISQtde", "STRING", mode="NULLABLE"),
    # Adicionar a coluna de partição
    bigquery.SchemaField("data_emissao", "STRING", mode="NULLABLE"),
]

# Criar tabela com configuração externa
table = bigquery.Table(table_ref, schema=schema)
table.external_data_configuration = external_config

# Criar ou atualizar a tabela
table = client_bq.create_table(table, exists_ok=True)

print(f"Tabela externa criada: {table_ref}")
print(f"Origem: {GCS_URI}*")
print("test")
