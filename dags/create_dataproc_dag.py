"""
DAG para ejecutar pipeline de analytics en Dataproc con cluster efímero
Proyecto: via-gcb-ia-tributario-hlg
Composer Version: 3
Ejecución: Diaria a las 2 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.dates import days_ago
from airflow.models.variable import Variable

from utils.date_utils import obter_pasta_processamento

# Carregar variáveis de ambiente do arquivo .env
# 1. LER AS CONFIGURAÇÕES DA VARIÁVEL DO AIRFLOW
# ==================================================================
try:
    # Nossa única fonte da verdade para todo o workflow
    CONFIG = Variable.get("dataproc_config_Dataproc", deserialize_json=True)
except Exception as e:
    print(f"Could not load Airflow Variable: {e}")
    CONFIG = {}


# ============================================
# CONFIGURACIÓN CENTRALIZADA
# ============================================

PROJECT_ID = CONFIG.get('PROJECT_ID')
REGION = CONFIG.get('REGION')
CLUSTER_PREFIX = CONFIG.get('CLUSTER_NAME')
GCS_NAME = CONFIG.get('GCS_NAME')
CLUSTER_NAME = f"{CLUSTER_PREFIX}-{{{{ ds_nodash }}}}"

# Pega a configuração específica do job da API da nossa variável
API_JOB_ARGS_CONFIG = CONFIG.get("API_EXTRACT_JOB", {})

ENTORNO = CONFIG.get("ENTORNO")

# Rutas de los scripts PySpark en GCS
SCRIPTS_BASE_PATH = f"gs://{GCS_NAME}/dags"
JOBS_CONFIG = [
    {
        "job_id": "extraction_xml_api",
        "script": f"{SCRIPTS_BASE_PATH}/jobs/extract_xml_dataproc_fast.py",
        "description": "Extrai os XMLs da API de documentos fiscais",
        "args": [
            "--gcs_bucket",       API_JOB_ARGS_CONFIG.get("GCS_BUCKET"),
            "--source_csv",       API_JOB_ARGS_CONFIG.get("SOURCE_CSV"),
            "--state_file",       API_JOB_ARGS_CONFIG.get("STATE_FILE"),
            "--output_prefix",    API_JOB_ARGS_CONFIG.get("OUTPUT_PREFIX"),
            # "--output_prefix", "{{ obter_pasta_processamento(datetime.strptime(ds, '%Y-%m-%d').date()) }}" if ENTORNO == "PROD" else API_JOB_ARGS_CONFIG.get("OUTPUT_PREFIX"),
            "--max_workers",      "4",
            "--batch_size",       "500",
            "--max_upload_workers", "32"
        ]
    },
    {
        "job_id": "processing_nfe",
        "script": f"{SCRIPTS_BASE_PATH}/jobs/process_nfe_xmls.py",
        "description": "Processa os XMLs de NFe e gera o Parquet",
        "args": [
            "--entorno", ENTORNO
            # "--input_prefix_folder", "{{ obter_pasta_processamento(datetime.strptime(ds, '%Y-%m-%d').date()) }}" if ENTORNO == "PROD" else "input_test"
        ]
    },
    {
        "job_id": "create_table",
        "script": f"{SCRIPTS_BASE_PATH}/jobs/create_table_external_parquet.py",
        "description": "Create table",
        "args": [
            "--entorno",    ENTORNO,
            "--project_id", CONFIG.get("PROJECT_ID"),
            "--gcs_name",   CONFIG.get("GCS_NAME"),
            "--gcs_name_parquet",   CONFIG.get("GCS_NAME_PARQUET")
        ]
    }
]

# Configuración del cluster (basada en tu YAML)
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_size_gb": 250
        },
        "image_uri": "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/global/images/dataproc-2-2-deb12-20251030-045100-rc02"
    },
    "worker_config": {
        "num_instances": 4,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_size_gb": 250
        },
        "image_uri": "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/global/images/dataproc-2-2-deb12-20251030-045100-rc02"
    },
    "software_config": {
        "image_version": "2.2.71-debian12",
        "optional_components": ["DOCKER"],
        "properties": {
            "spark:spark.executor.memory": "6157m",
            "spark:spark.executor.cores": "2",
            "spark:spark.executor.instances": "2",
            "spark:spark.driver.memory": "4096m",
            "spark:spark.driver.maxResultSize": "2048m",
            "spark:spark.scheduler.mode": "FAIR",
            "spark:spark.sql.cbo.enabled": "true",
            "yarn:yarn.nodemanager.resource.memory-mb": "13544",
            "yarn:yarn.nodemanager.resource.cpu-vcores": "4"
        }
    },
    "initialization_actions": [
        {
            "executable_file": f"{SCRIPTS_BASE_PATH}/scripts/setup_dependecies.sh",
            "execution_timeout": {"seconds": 600}
        }
    ],
    "gce_cluster_config": {
        "service_account": "sa-via-gcb-ia-tributario-hlg@via-gcb-ia-tributario-hlg.iam.gserviceaccount.com",
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write"
        ],
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/via-gcb-ia-tributario-hlg/zones/us-central1-a",
        "subnetwork_uri": "https://www.googleapis.com/compute/v1/projects/via-corp-net-shared-hlg/regions/us-central1/subnetworks/sn-via-corp-net-shared-hlg-ia-tributario-hlg",
        "internal_ip_only": True,
        "tags": ["dataproc-cluster", "ssh"],
        "shielded_instance_config": {
            "enable_secure_boot": True,
            "enable_vtpm": True,
            "enable_integrity_monitoring": True
        }
    },
    "endpoint_config": {
        "enable_http_port_access": True
    },
    "temp_bucket": "dataproc-temp-us-central1-1830289231-vck45zg1",
    "config_bucket": "dataproc-staging-us-central1-1830289231-zzyjclcg"
}

# Labels del cluster
CLUSTER_LABELS = {
    "environment": "homologacao",
    "team": "ia-tributario"
}

# ============================================
# DEFINICIÓN DEL DAG
# ============================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dataproc_analytics_pipeline",
    default_args=default_args,
    description="Pipeline de analytics con Dataproc efímero - Ejecución secuencial de jobs PySpark",
    schedule_interval="0 2 * * *",  # Diario a las 2 AM
    # schedule_interval=None,
    catchup=False,
    tags=["dataproc", "pyspark", "analytics", "ephemeral"],
    user_defined_macros={
        'obter_pasta_processamento': obter_pasta_processamento,
        'datetime': datetime
    }
) as dag:

    # ============================================
    # TAREA 1: CREAR CLUSTER EFÍMERO
    # ============================================
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        labels=CLUSTER_LABELS,
    )

    # ==================================================================
    # TAREFA DE LIMPEZA, DEFINIDA FORA DO LOOP
    # ==================================================================
    limpar_subpasta_parquet = GCSDeleteObjectsOperator(
        task_id="submit_job_clean_parquet_subfolder",
        bucket_name=CONFIG.get("GCS_NAME_PARQUET"),
        prefix="xml1/" if CONFIG.get("ENTORNO") == "DEV" else "xml/",
    )

    # ============================================
    # TAREA 2-4: EJECUTAR JOBS PYSPARK
    # ============================================
    previous_task = create_cluster

    for job_config in JOBS_CONFIG:

        pyspark_job = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": job_config["script"],
                "args": job_config.get("args", [])
            }
        }

        submit_job = DataprocSubmitJobOperator(
            task_id=f"submit_job_{job_config['job_id']}",
            job=pyspark_job,
            region=REGION,
            project_id=PROJECT_ID,
        )

        # Se encontrarmos o job que vem DEPOIS da limpeza, ajustamos a dependência
        if job_config['job_id'] == 'processing_nfe':
            # A tarefa anterior (job da API) deve levar à limpeza
            previous_task >> limpar_subpasta_parquet
            # A limpeza agora se torna a "tarefa anterior" para o job de NFe
            previous_task = limpar_subpasta_parquet

        # Establecer dependencia secuencial
        previous_task >> submit_job
        previous_task = submit_job

    # ============================================
    # TAREA 5: ELIMINAR CLUSTER
    # ============================================
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # Se ejecuta siempre, incluso si falla algún job
    )

    # Conectar el último job con la eliminación del cluster
    previous_task >> delete_cluster

# ============================================
# FLUJO DEL DAG
# ============================================
# create_cluster >> submit_job_api_connection >> submit_job_tags_extraction >>
# submit_job_vertex_execution >> delete_cluster
