"""
Data: 2026-03-19
Autor: Claude Code
Descrição: Refina respostas usando feedbacks do BigQuery + RAG com Gemini
Refatorado para Dataproc Serverless em 2026-04-07
"""

import time
from datetime import datetime
import argparse
import google.genai as genai
import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

# ============================================
# ARGUMENTOS (substitui dotenv)
# ============================================
parser = argparse.ArgumentParser(description='Refina respostas IA com feedbacks')
parser.add_argument("--project_id", required=True, help="ID do projeto GCP")
parser.add_argument("--location", required=True, help="Região do Vertex AI")
parser.add_argument("--gemini_model", required=True, help="Nome do modelo Gemini")
parser.add_argument("--bucket_file_path", required=True, help="Caminho GCS da base de conhecimento")
parser.add_argument("--output_bucket_path", required=True, help="Caminho GCS para outputs")
parser.add_argument("--bigquery_dataset", required=True, help="Dataset BigQuery")
parser.add_argument("--bigquery_table_feedback", required=True, help="Tabela BigQuery feedbacks")
args = parser.parse_args()

# Variáveis globais
PROJECT_ID = args.project_id
LOCATION = args.location
GEMINI_MODEL = args.gemini_model
BUCKET_FILE_PATH = args.bucket_file_path
OUTPUT_BUCKET_PATH = args.output_bucket_path
BIGQUERY_DATASET = args.bigquery_dataset
BIGQUERY_TABLE_FEEDBACK = args.bigquery_table_feedback

# Credenciais usando Application Default Credentials (ADC)
from google.auth import default
creds, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])


def init_gemini():
    """Inicializa o cliente Gemini"""
    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION,
        credentials=creds
    )
    return client


def buscar_feedbacks_com_sugestao(client_bq):
    """
    Busca feedbacks do BigQuery onde usuário deu sugestão
    EXCLUI erros que já têm 10+ feedbacks positivos (consolidados)
    """
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FEEDBACK}"

    query = f"""
    WITH erros_consolidados AS (
      -- Identifica erros que já têm 10+ feedbacks positivos
      SELECT
        id_error,
        COUNT(*) as total_feedbacks_positivos
      FROM `{table_id}`
      WHERE flag_causa_raiz_util = TRUE AND flag_correcao_util = TRUE
      GROUP BY id_error
      HAVING COUNT(*) >= 10 
    )
    SELECT
      f.id_feedback,
      f.id_error,
      f.inconsistencia,
      f.causa_raiz_gerada,
      f.correcao_gerada,
      f.flag_causa_raiz_util,
      f.flag_correcao_util,
      f.sugestao_causa_raiz,
      f.sugestao_correcao,
      f.usuario,
      f.data_feedback
    FROM `{table_id}` f
    LEFT JOIN erros_consolidados ec ON f.id_error = ec.id_error
    WHERE
      ec.id_error IS NULL  -- Exclui erros consolidados
      AND (
        (f.flag_causa_raiz_util = FALSE AND f.sugestao_causa_raiz IS NOT NULL AND f.sugestao_causa_raiz != '')
        OR
        (f.flag_correcao_util = FALSE AND f.sugestao_correcao IS NOT NULL AND f.sugestao_correcao != '')
      )
    ORDER BY f.data_feedback DESC
    """

    print("🔍 Buscando feedbacks com sugestões no BigQuery...")
    print("   (Excluindo erros já CONSOLIDADOS com 10+ feedbacks positivos)")

    try:
        df = client_bq.query(query).to_dataframe()
        print(f"✅ Feedbacks encontrados: {len(df)}")

        if not df.empty:
            # Estatísticas
            feedbacks_causa = len(df[
                (df['flag_causa_raiz_util'] == False) &
                (df['sugestao_causa_raiz'].notna()) &
                (df['sugestao_causa_raiz'] != '')
            ])
            feedbacks_correcao = len(df[
                (df['flag_correcao_util'] == False) &
                (df['sugestao_correcao'].notna()) &
                (df['sugestao_correcao'] != '')
            ])

            print(f"\n📊 ANÁLISE:")
            print(f"   Sugestões para Causa Raiz: {feedbacks_causa}")
            print(f"   Sugestões para Correção: {feedbacks_correcao}")
            print(f"   Erros únicos: {df['id_error'].nunique()}")

        return df

    except Exception as e:
        print(f"❌ Erro ao buscar feedbacks: {str(e)}")
        print("   💡 Certifique-se de que a tabela existe e tem dados")
        return pd.DataFrame()


def refinar_com_gemini(client_gemini, feedback_row, base_conhecimento):
    """
    Usa Gemini + RAG para refinar resposta baseado na sugestão do usuário
    """
    id_error = feedback_row['id_error']
    causa_raiz_antiga = feedback_row['causa_raiz_gerada']
    correcao_antiga = feedback_row['correcao_gerada']

    # Sugestões do usuário
    sugestao_causa = feedback_row.get('sugestao_causa_raiz', '')
    sugestao_correcao = feedback_row.get('sugestao_correcao', '')
    flag_causa_util = feedback_row.get('flag_causa_raiz_util', True)
    flag_correcao_util = feedback_row.get('flag_correcao_util', True)

    # Monta contexto da base de conhecimento (para manter padrão)
    context = "=== BASE DE CONHECIMENTO (REFERÊNCIA DE ESTILO) ===\n\n"
    for i, (idx, row) in enumerate(base_conhecimento.head(10).iterrows(), 1):
        context += f"Exemplo {i}:\n"
        context += f"- Descrição: {row['description']}\n"
        context += f"- Causa Raiz: {row['causa_raiz']}\n"
        context += f"- Correção: {row['correcao']}\n\n"

    # Monta prompt de refinamento
    prompt = f"""
Você é um especialista em erros fiscais tributários brasileiros.

{context}

=== ERRO ID {id_error}: {feedback_row['inconsistencia']} ===

RESPOSTA ORIGINAL (GERADA PELA IA):
Causa Raiz: {causa_raiz_antiga}
Correção: {correcao_antiga}

"""

    # Adiciona feedback de causa raiz se não foi útil
    if not flag_causa_util and sugestao_causa:
        prompt += f"""
❌ FEEDBACK NEGATIVO - CAUSA RAIZ
O usuário ({feedback_row['usuario']}) rejeitou a causa raiz e sugeriu:
"{sugestao_causa}"

TAREFA 1: REFINE A CAUSA RAIZ
- Use a sugestão do usuário como guia principal
- Mantenha o estilo técnico e preciso dos exemplos acima
- Seja objetivo (2-3 frases)
"""

    # Adiciona feedback de correção se não foi útil
    if not flag_correcao_util and sugestao_correcao:
        prompt += f"""
❌ FEEDBACK NEGATIVO - CORREÇÃO
O usuário ({feedback_row['usuario']}) rejeitou a correção e sugeriu:
"{sugestao_correcao}"

TAREFA 2: REFINE A CORREÇÃO
- Use a sugestão do usuário como guia principal
- Seja específico e prático (passo a passo)
- Mantenha o padrão dos exemplos acima
"""

    prompt += """

FORMATO DE RESPOSTA OBRIGATÓRIO:
CAUSA_RAIZ_REFINADA: [sua versão refinada]
CORRECAO_REFINADA: [sua versão refinada]

Seja direto e técnico.
"""

    try:
        response = client_gemini.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )

        texto = response.text.strip()

        # Parse da resposta
        causa_refinada = causa_raiz_antiga  # Mantém original como fallback
        correcao_refinada = correcao_antiga

        if "CAUSA_RAIZ_REFINADA:" in texto:
            causa_parte = texto.split("CAUSA_RAIZ_REFINADA:")[1].split("CORRECAO_REFINADA:")[0].strip()
            if causa_parte and not flag_causa_util:
                causa_refinada = causa_parte

        if "CORRECAO_REFINADA:" in texto:
            correcao_parte = texto.split("CORRECAO_REFINADA:")[1].strip()
            if correcao_parte and not flag_correcao_util:
                correcao_refinada = correcao_parte

        return {
            "causa_raiz": causa_refinada,
            "correcao": correcao_refinada,
            "origem": "REFINADO_FEEDBACK",
            "usuario_feedback": feedback_row['usuario'],
            "data_refinamento": datetime.now().isoformat()
        }

    except Exception as e:
        print(f"   ⚠️ Erro ao refinar ID {id_error}: {str(e)}")
        return {
            "causa_raiz": causa_raiz_antiga,
            "correcao": correcao_antiga,
            "origem": "ERRO_REFINAMENTO",
            "usuario_feedback": feedback_row['usuario'],
            "data_refinamento": datetime.now().isoformat()
        }


def main():
    # Marca início
    tempo_inicio = time.time()

    print("=" * 80)
    print("🔄 SCRIPT 3: REFINADOR COM RAG")
    print("📋 Refina respostas usando feedbacks + Gemini")
    print("=" * 80)
    print()

    # Inicializa clientes
    print("⚙️  Inicializando clientes...")
    client_gemini = init_gemini()
    client_bq = bigquery.Client(credentials=creds, project=PROJECT_ID)
    print("✅ Clientes inicializados\n")

    # Carrega base de conhecimento (para contexto)
    print("📂 Carregando base de conhecimento...")
    try:
        df_base = pd.read_csv(
            BUCKET_FILE_PATH,
            sep=';',  # Base original usa ponto e vírgula
            storage_options={"token": creds}
        )
        print(f"✅ Base carregada: {len(df_base)} erros\n")
    except Exception as e:
        print(f"⚠️ Erro ao carregar base: {str(e)}")
        print("   Continuando sem contexto da base...\n")
        df_base = pd.DataFrame()

    # Busca feedbacks com sugestão
    df_feedbacks = buscar_feedbacks_com_sugestao(client_bq)

    if df_feedbacks.empty:
        print("\n⚠️ Nenhum feedback com sugestão encontrado!")
        print("   Isso é normal se:")
        print("   1. É a primeira execução")
        print("   2. Todos os feedbacks foram positivos")
        print("   3. Usuários não deram sugestões")
        return

    # Processa cada feedback
    print(f"\n🔄 Refinando {len(df_feedbacks)} respostas...\n")

    resultados = []

    for idx, feedback in tqdm(df_feedbacks.iterrows(), total=len(df_feedbacks), desc="Refinando"):
        id_error = feedback['id_error']
        print(f"\n  🔧 ID {id_error}: Refinando com feedback de {feedback['usuario']}")

        resultado = refinar_com_gemini(client_gemini, feedback, df_base)

        resultados.append({
            'id_error': id_error,
            'description': feedback['inconsistencia'],
            'causa_raiz': resultado['causa_raiz'],
            'correcao': resultado['correcao'],
            'origem': resultado['origem'],
            'usuario_feedback': resultado['usuario_feedback'],
            'data_refinamento': resultado['data_refinamento']
        })

    # Cria DataFrame
    df_refinado = pd.DataFrame(resultados)

    # Salva no bucket
    output_filename = f"base_refinada_{datetime.now().strftime('%Y%m%d')}.csv"
    output_path = OUTPUT_BUCKET_PATH.rstrip('/') + '/' + output_filename

    print(f"\n💾 Salvando respostas refinadas...")
    df_refinado.to_csv(
        output_path,
        sep=',',
        index=False,
        encoding='utf-8',
        storage_options={"token": creds}
    )

    # Calcula tempo
    tempo_fim = time.time()
    tempo_total = tempo_fim - tempo_inicio
    tempo_formatado = time.strftime("%H:%M:%S", time.gmtime(tempo_total))

    print("\n" + "=" * 80)
    print("✅ REFINAMENTO CONCLUÍDO!")
    print("=" * 80)
    print(f"\n📊 ESTATÍSTICAS:")
    print(f"   Feedbacks processados: {len(df_feedbacks)}")
    print(f"   Respostas refinadas: {len(df_refinado)}")
    print(f"   Erros únicos refinados: {df_refinado['id_error'].nunique()}")
    print(f"\n📁 Arquivo gerado: {output_path}")
    print(f"⏱️  Tempo de execução: {tempo_formatado} ({tempo_total:.2f} segundos)")
    print("\n💡 PRÓXIMO PASSO:")
    print("   Execute novamente: uv run src/Teste_gio/1_generate_all_errors.py")
    print("   O Script 1 agora vai usar as respostas refinadas!")
    print("=" * 80)


if __name__ == "__main__":
    main()
