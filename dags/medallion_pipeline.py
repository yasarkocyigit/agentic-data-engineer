"""
Metadata-Driven Medallion Pipeline DAG
=======================================
Reads pipeline config from YAML (config/pipelines.yml).
Submits Spark jobs to the remote Spark cluster using SparkSubmitOperator.

This follows enterprise patterns where Airflow acts as an orchestrator
and delegates compute to a remote Spark cluster via spark-submit.

Flow:
  1. read_config → Parse YAML config
  2. run_bronze  → SparkSubmitOperator → Bronze ingestion (Parquet)
  3. run_silver  → SparkSubmitOperator → Silver transformations (Iceberg)
  4. log_result  → Audit log
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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

# JAR files for PostgreSQL JDBC, Hadoop-AWS, AWS SDK, and Iceberg runtime
# Iceberg runtime is needed because PySpark pip package doesn't bundle it.
SPARK_JARS = ",".join([
    "/opt/spark/extra-jars/postgresql-42.7.5.jar",
    "/opt/spark/extra-jars/hadoop-aws-3.4.1.jar",
    "/opt/spark/extra-jars/bundle-2.31.1.jar",
    "/opt/spark/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
])

# Spark configuration for Iceberg + MinIO
SPARK_CONF = {
    # Iceberg catalog (JDBC-based, metadata stored in PostgreSQL)
    # Shared between Spark and Trino so both engines see the same tables
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "jdbc",
    "spark.sql.catalog.lakehouse.uri": "jdbc:postgresql://host.docker.internal:5433/controldb",
    "spark.sql.catalog.lakehouse.jdbc.user": "postgres",
    "spark.sql.catalog.lakehouse.jdbc.password": "Gs+163264128",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://warehouse/",
    # MinIO / S3A configuration
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "admin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}


# =============================================================
# HELPER FUNCTIONS
# =============================================================

def read_pipeline_config(**context):
    """Read active pipeline config from YAML files."""
    config_path = "/opt/airflow/config/pipelines.yml"

    # Fallback for local dev
    if not os.path.exists(config_path):
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "config", "pipelines.yml"
        )

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
    print(f"📋 Pipeline Config:")
    print(f"   Bronze: {len(bronze)} tables — {summary['bronze_tables']}")
    print(f"   Silver: {len(silver)} tables — {summary['silver_tables']}")

    return summary


def log_result(status: str, **context):
    """Log execution result."""
    summary = context['ti'].xcom_pull(
        task_ids='read_config', key='pipeline_summary'
    )
    run_id = context.get('run_id', 'unknown')

    print(f"{'='*50}")
    print(f"  Pipeline {status.upper()}")
    print(f"  Run ID: {run_id}")
    print(f"  Tables: {summary}")
    print(f"{'='*50}")


# =============================================================
# DAG DEFINITION
# =============================================================

with DAG(
    dag_id='medallion_pipeline',
    default_args=default_args,
    description='Medallion Pipeline: Bronze (Parquet) → Silver (Iceberg) — SparkSubmitOperator',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'lakehouse', 'agentic-ai', 'tpch', 'iceberg'],
    max_active_runs=1,
) as dag:

    # ─── Task 1: Read YAML config ───────────────────────────
    read_config = PythonOperator(
        task_id='read_config',
        python_callable=read_pipeline_config,
    )

    # ─── Task 2: Bronze Ingestion (PostgreSQL → MinIO/Parquet) ─
    run_bronze = SparkSubmitOperator(
        task_id='run_bronze_ingestion',
        application='/opt/airflow/notebooks/bronze/ingestion.py',
        conn_id='spark_default',
        conf=SPARK_CONF,
        jars=SPARK_JARS,
        deploy_mode='client',
        execution_timeout=timedelta(hours=1),
        verbose=True,
    )

    # ─── Task 3: Silver Transforms (Parquet → Iceberg) ───────
    run_silver = SparkSubmitOperator(
        task_id='run_silver_transforms',
        application='/opt/airflow/notebooks/silver/transformations.py',
        conn_id='spark_default',
        conf=SPARK_CONF,
        jars=SPARK_JARS,
        deploy_mode='client',
        execution_timeout=timedelta(hours=1),
        verbose=True,
    )

    # ─── Task 4: Log success ────────────────────────────────
    log_success = PythonOperator(
        task_id='log_success',
        python_callable=lambda **ctx: log_result('success', **ctx),
        trigger_rule='all_success',
    )

    # ─── Task 5: Log failure ────────────────────────────────
    log_failure = PythonOperator(
        task_id='log_failure',
        python_callable=lambda **ctx: log_result('failed', **ctx),
        trigger_rule='one_failed',
    )

    # ─── DAG Flow ────────────────────────────────────────────
    # read_config → run_bronze → run_silver → log_success
    #                                        ↘ log_failure
    read_config >> run_bronze >> run_silver
    run_silver >> [log_success, log_failure]
