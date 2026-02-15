"""
Metadata-Driven Medallion Pipeline DAG
=======================================
Reads pipeline config from YAML (config/pipelines.yml).
Submits Spark jobs to the remote Spark cluster using SparkSubmitOperator.

This follows enterprise patterns where Airflow acts as an orchestrator
and delegates compute to a remote Spark cluster via spark-submit.

Flow:
  1. read_config → Parse YAML config
  2. run_bronze  → BashOperator → Bronze ingestion (Parquet)
  3. run_silver  → BashOperator → Silver transformations (Iceberg)
  4. log_result  → Audit log
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import yaml
import os

# =============================================================
# DAG DEFAULT ARGS
# =============================================================

default_args = {
    'owner': 'agentic-ai',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=2),
}

# =============================================================
# SPARK CONFIGURATION
# =============================================================

SPARK_MASTER = "spark://spark-master:7077"

# JAR files
JARS = ",".join([
    "/opt/spark/extra-jars/postgresql-42.7.5.jar",
    "/opt/spark/extra-jars/hadoop-aws-3.4.1.jar",
    "/opt/spark/extra-jars/bundle-2.31.1.jar",
    "/opt/spark/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
])

# Spark conf as CLI string
SPARK_CONF_CMD = (
    f"--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog "
    f"--conf spark.sql.catalog.lakehouse.type=jdbc "
    f"--conf spark.sql.catalog.lakehouse.uri={os.getenv('ICEBERG_CATALOG_URI', 'jdbc:postgresql://host.docker.internal:5433/controldb')} "
    f"--conf spark.sql.catalog.lakehouse.jdbc.user={os.getenv('POSTGRES_USER', 'postgres')} "
    f"--conf spark.sql.catalog.lakehouse.jdbc.password={os.getenv('POSTGRES_PASSWORD', 'changeme')} "
    f"--conf spark.sql.catalog.lakehouse.warehouse=s3a://warehouse/ "
    f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    f"--conf spark.hadoop.fs.s3a.access.key={os.getenv('MINIO_ROOT_USER', 'admin')} "
    f"--conf spark.hadoop.fs.s3a.secret.key={os.getenv('MINIO_ROOT_PASSWORD', 'YOUR_MINIO_PASSWORD_HERE')} "
    f"--conf spark.hadoop.fs.s3a.path.style.access=true "
    f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    f"--jars {JARS} "
    f"--verbose "
)


# =============================================================
# HELPER FUNCTIONS
# =============================================================

def read_pipeline_config(**context):
    """Read active pipeline config from YAML files."""
    config_path = "/opt/airflow/config/pipelines.yml"
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "pipelines.yml")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    bronze = [p for p in config.get("bronze", []) if p.get("is_active", True)]
    silver = [p for p in config.get("silver", []) if p.get("is_active", True)]

    summary = {
        "bronze_count": len(bronze),
        "silver_count": len(silver),
        "bronze_tables": [p["name"] for p in bronze],
        "silver_tables": [p["name"] for p in silver],
    }

    context['ti'].xcom_push(key='pipeline_summary', value=summary)
    print(f"📋 Pipeline Config: Bronze={len(bronze)}, Silver={len(silver)}")
    return summary


def log_result(status: str, **context):
    """Log execution result."""
    summary = context['ti'].xcom_pull(task_ids='read_config', key='pipeline_summary')
    run_id = context.get('run_id', 'unknown')
    print(f"Pipeline {status.upper()} | Run ID: {run_id} | Tables: {summary}")


# =============================================================
# DAG DEFINITION
# =============================================================

with DAG(
    dag_id='medallion_pipeline',
    default_args=default_args,
    description='Medallion Pipeline: Bronze (Parquet) → Silver (Iceberg) — BashOperator',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'lakehouse', 'agentic-ai', 'tpch', 'iceberg'],
    max_active_runs=1,
) as dag:

    read_config = PythonOperator(
        task_id='read_config',
        python_callable=read_pipeline_config,
    )

    # Use BashOperator to avoid SparkSubmitOperator connection issues
    run_bronze = BashOperator(
        task_id='run_bronze_ingestion',
        bash_command=f"/home/airflow/.local/bin/spark-submit --master {SPARK_MASTER} {SPARK_CONF_CMD} /opt/airflow/notebooks/bronze/ingestion.py",
        env={
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "changeme"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "host.docker.internal"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5433"),
            "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "admin"),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "YOUR_MINIO_PASSWORD_HERE"),
            "ICEBERG_CATALOG_URI": os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://host.docker.internal:5433/controldb"),
        },
        execution_timeout=timedelta(hours=1),
    )

    run_silver = BashOperator(
        task_id='run_silver_transforms',
        bash_command=f"/home/airflow/.local/bin/spark-submit --master {SPARK_MASTER} {SPARK_CONF_CMD} /opt/airflow/notebooks/silver/transformations.py",
        env={
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "changeme"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "host.docker.internal"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5433"),
            "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "admin"),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "YOUR_MINIO_PASSWORD_HERE"),
            "ICEBERG_CATALOG_URI": os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://host.docker.internal:5433/controldb"),
        },
        execution_timeout=timedelta(hours=1),
    )

    log_success = PythonOperator(
        task_id='log_success',
        python_callable=lambda **ctx: log_result('success', **ctx),
        trigger_rule='all_success',
    )

    log_failure = PythonOperator(
        task_id='log_failure',
        python_callable=lambda **ctx: log_result('failed', **ctx),
        trigger_rule='one_failed',
    )

    read_config >> run_bronze >> run_silver
    run_silver >> [log_success, log_failure]

# Force re-parse Fri Feb 13 18:03:00 EST 2026
