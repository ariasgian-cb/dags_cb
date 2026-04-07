"""
Data: 2026-03-17
Autor: Claude Code
Descrição: Ingere CSV do bucket para BigQuery (tabela histórica)
Refatorado para Dataproc Serverless em 2026-04-07
"""

import os
import time
from datetime import datetime
import argparse
import pandas as pd
from google.cloud import bigquery

# ============================================
# ARGUMENTOS (substitui dotenv)
# ============================================
parser = argparse.ArgumentParser(description='Ingere respostas IA no BigQuery')
parser.add_argument("--project_id", required=True, help="ID do projeto GCP")
parser.add_argument("--output_bucket_path", required=True, help="Caminho GCS dos outputs")
parser.add_argument("--bigquery_dataset", required=True, help="Dataset BigQuery")
parser.add_argument("--bigquery_table", required=True, help="Tabela BigQuery histórico")
args = parser.parse_args()

# Variáveis globais
PROJECT_ID = args.project_id
OUTPUT_BUCKET_PATH = args.output_bucket_path
BIGQUERY_DATASET = args.bigquery_dataset
BIGQUERY_TABLE = args.bigquery_table

# Credenciais usando Application Default Credentials (ADC)
from google.auth import default
creds, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])


def load_csv_from_bucket(data_execucao=None):
    """Carrega CSV gerado do bucket"""
    try:
        if data_execucao:
            filename = f"CSV_output_{data_execucao}.csv"
        else:
            # Usa data de hoje
            filename = f"CSV_output_{datetime.now().strftime('%Y-%m-%d')}.csv"

        csv_path = OUTPUT_BUCKET_PATH.rstrip('/') + '/' + filename

        print(f"📂 Carregando: {csv_path}")

        df = pd.read_csv(
            csv_path,
            sep=',',  # Alterado para vírgula (alinhado com script 1)
            storage_options={"token": creds}
        )

        print(f"✅ CSV carregado: {len(df)} registros")
        return df, csv_path

    except Exception as e:
        print(f"❌ Erro ao carregar CSV: {str(e)}")
        return pd.DataFrame(), None


def criar_tabela_se_nao_existe(client):
    """Cria tabela no BigQuery se não existir"""
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    # Schema da tabela
    schema = [
        bigquery.SchemaField("id_error", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_regra", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("causa_raiz", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("correcao", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("data_geracao", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("data_execucao", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("versao", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("origem", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("confianca", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hash_resposta", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    # Configurações de particionamento e clustering
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="data_execucao"
    )
    table.clustering_fields = ["id_error", "id_regra"]

    try:
        client.get_table(table_id)
        print(f"✅ Tabela já existe: {table_id}")
    except Exception:
        table = client.create_table(table)
        print(f"✅ Tabela criada: {table_id}")


def ingerir_dados(client, df):
    """Faz APPEND dos dados na tabela BigQuery"""
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    print(f"\n🔄 Ingerindo {len(df)} registros no BigQuery...")

    # Converte colunas de data
    df['data_execucao'] = pd.to_datetime(df['data_execucao'])
    df['data_geracao'] = pd.to_datetime(df['data_geracao']).dt.date  # Converte para date

    # Configuração do job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # APPEND (não sobrescreve)
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )

    # Carrega dados
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    # Aguarda conclusão
    job.result()

    print(f"✅ Dados ingeridos com sucesso!")
    print(f"   Total de linhas na tabela: {client.get_table(table_id).num_rows}")



def main():
    # Marca início da execução
    tempo_inicio = time.time()

    print("=" * 80)
    print("📊 SCRIPT 2: INGESTÃO NO BIGQUERY")
    print("📋 Ingere CSV gerado na tabela histórica")
    print("=" * 80)
    print()

    # Inicializa cliente BigQuery
    print("⚙️  Inicializando BigQuery...")
    client = bigquery.Client(credentials=creds, project=PROJECT_ID)
    print("✅ Cliente inicializado\n")

    # Carrega CSV do bucket
    df, csv_path = load_csv_from_bucket()

    if df.empty:
        print("❌ Nenhum dado para ingerir")
        return

    # Cria tabela se não existe
    print("\n📦 Verificando/criando tabela...")
    criar_tabela_se_nao_existe(client)

    # Ingere dados (APPEND)
    ingerir_dados(client, df)

    # Calcula tempo de execução
    tempo_fim = time.time()
    tempo_total = tempo_fim - tempo_inicio
    tempo_formatado = time.strftime("%H:%M:%S", time.gmtime(tempo_total))

    print("\n" + "=" * 80)
    print("✅ INGESTÃO CONCLUÍDA!")
    print("=" * 80)
    print(f"\n📊 INFORMAÇÕES:")
    print(f"   CSV origem: {csv_path}")
    print(f"   Tabela destino: {PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}")
    print(f"   Registros ingeridos: {len(df)}")
    print(f"   ⏱️  Tempo de execução: {tempo_formatado} ({tempo_total:.2f} segundos)")
    print("\n💡 PRÓXIMOS PASSOS:")
    print("   1. Criar VIEW manualmente no BigQuery (se necessário)")
    print("   2. Conectar Looker à tabela ou VIEW")
    print("   3. Coletar feedbacks dos usuários")
    print("   4. Executar: python src/Teste_gio/3_export_feedback_to_bucket.py")
    print("=" * 80)


if __name__ == "__main__":
    main()
