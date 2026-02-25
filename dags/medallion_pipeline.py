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
from airflow.sdk import Asset
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
    "spark.sql.catalog.lakehouse.uri": os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://host.docker.internal:5433/controldb"),
    "spark.sql.catalog.lakehouse.jdbc.user": os.getenv("ICEBERG_CATALOG_USER", "postgres"),
    "spark.sql.catalog.lakehouse.jdbc.password": os.getenv("ICEBERG_CATALOG_PASSWORD", "Gs+163264128"),
    "spark.sql.catalog.lakehouse.warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg/warehouse/"),
    # MinIO / S3A configuration
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER", "admin"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD", "admin123"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}


# =============================================================
# HELPER FUNCTIONS
# =============================================================

def resolve_pipeline_config_path() -> str:
    """Resolve pipelines.yml path for container and local development."""
    config_path = "/opt/airflow/config/pipelines.yml"
    if os.path.exists(config_path):
        return config_path
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "config",
        "pipelines.yml",
    )


def load_active_pipelines():
    """Load active Bronze/Silver pipeline definitions from config."""
    config_path = resolve_pipeline_config_path()
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    bronze = [p for p in config.get("bronze", []) if p.get("is_active", True)]
    silver = [p for p in config.get("silver", []) if p.get("is_active", True)]
    return bronze, silver


def normalize_asset_uri(uri: str) -> str:
    """Normalize URI to schemes supported by OpenLineage asset converters."""
    value = (uri or "").strip()
    if not value:
        return value
    # OpenLineage Airflow converter supports s3 but not s3a.
    if value.startswith("s3a://"):
        return f"s3://{value[len('s3a://'):]}"
    return value


def source_table_asset_uri(source_table: str) -> str:
    """Build a file-based URI for JDBC source tables."""
    normalized = (source_table or "").strip().replace(".", "/")
    return f"file:///postgres/{normalized}"


def build_assets(uris: list[str]) -> list[Asset]:
    """Create deduplicated Airflow assets from URIs."""
    seen = set()
    assets: list[Asset] = []
    for uri in uris:
        normalized = normalize_asset_uri(uri)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        assets.append(Asset(uri=normalized))
    return assets


def build_lineage_assets(bronze_pipelines: list[dict], silver_pipelines: list[dict]):
    """Create inlets/outlets for Bronze and Silver tasks."""
    bronze_sources = [source_table_asset_uri(p.get("source_table", "")) for p in bronze_pipelines]
    bronze_targets = [p.get("target_path", "") for p in bronze_pipelines]
    silver_sources = [p.get("source", "") for p in silver_pipelines]
    silver_targets = [p.get("target_path", f"s3a://silver/{p.get('name', '')}") for p in silver_pipelines]

    return (
        build_assets(bronze_sources),
        build_assets(bronze_targets),
        build_assets(silver_sources),
        build_assets(silver_targets),
    )


BRONZE_PIPELINES, SILVER_PIPELINES = load_active_pipelines()
(
    BRONZE_INLETS,
    BRONZE_OUTLETS,
    SILVER_INLETS,
    SILVER_OUTLETS,
) = build_lineage_assets(BRONZE_PIPELINES, SILVER_PIPELINES)


def read_pipeline_config(**context):
    """Read active pipeline config from YAML files."""
    bronze, silver = load_active_pipelines()

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
        inlets=BRONZE_INLETS,
        outlets=BRONZE_OUTLETS,
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
        inlets=SILVER_INLETS,
        outlets=SILVER_OUTLETS,
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
