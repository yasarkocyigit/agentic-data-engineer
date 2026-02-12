"""
Bronze Layer - JDBC Ingestion to Parquet
============================================
Reads from PostgreSQL (TPC-H sourcedb) via JDBC.
Writes to MinIO (S3A) as Parquet files.

Config: config/pipelines.yml
Target: s3a://bronze/tpch/<table_name>

Usage:
    spark-submit --jars ... bronze/ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import yaml
import os
import sys
import time

# =============================================================
# SPARK SESSION
# =============================================================

spark = SparkSession.builder \
    .appName("Bronze-Ingestion-TPC-H") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  Bronze Layer — JDBC Ingestion (Parquet)")
print("=" * 60)


# =============================================================
# LOAD CONFIG FROM YAML
# =============================================================

# Config path - try multiple locations (Spark container, Airflow container, local)
config_candidates = [
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "pipelines.yml"),
    "/opt/spark/work/config/pipelines.yml",
    "/opt/airflow/config/pipelines.yml",
]
config_path = None
for candidate in config_candidates:
    if os.path.exists(candidate):
        config_path = candidate
        break

if config_path is None:
    raise FileNotFoundError(f"Could not find pipelines.yml in any of: {config_candidates}")

with open(config_path) as f:
    config = yaml.safe_load(f)

bronze_pipelines = [p for p in config.get("bronze", []) if p.get("is_active", True)]
print(f"\n📋 Active Bronze pipelines: {len(bronze_pipelines)}")

# Load connection config
conn_path = os.path.join(os.path.dirname(config_path), "connections.yml")
with open(conn_path) as f:
    connections = yaml.safe_load(f)


# =============================================================
# RESOLVE CONNECTION
# =============================================================

def resolve_env(value):
    """Resolve ${ENV_VAR:default} patterns."""
    import re
    if not isinstance(value, str):
        return value
    def replacer(m):
        expr = m.group(1)
        if ":" in expr:
            var, default = expr.split(":", 1)
        else:
            var, default = expr, ""
        return os.getenv(var, default)
    return re.sub(r'\$\{([^}]+)\}', replacer, value)


conn_name = bronze_pipelines[0]["connection"] if bronze_pipelines else "postgres_sourcedb"
conn = connections.get(conn_name, {})

jdbc_url = resolve_env(conn.get("url", ""))
jdbc_user = resolve_env(conn.get("user", "postgres"))
jdbc_password = resolve_env(conn.get("password", ""))
jdbc_driver = conn.get("driver", "org.postgresql.Driver")
fetchsize = str(conn.get("options", {}).get("fetchsize", 10000))

print(f"🔗 Connection: {conn_name}")
print(f"   URL: {jdbc_url}")
print(f"   Driver: {jdbc_driver}")


# =============================================================
# BRONZE INGESTION LOOP
# =============================================================

total_rows = 0
start_time = time.time()
results = []

for pipeline in bronze_pipelines:
    p_name = pipeline["name"]
    p_source = pipeline["source_table"]
    p_target = pipeline["target_path"]
    p_pk = pipeline.get("primary_key", "")

    print(f"\n{'─'*50}")
    print(f"  📥 Ingesting: {p_name}")
    print(f"     Source: {p_source}")
    print(f"     Target: {p_target}")

    try:
        t0 = time.time()

        # Read from PostgreSQL via JDBC
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", p_source) \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .option("driver", jdbc_driver) \
            .option("fetchsize", fetchsize) \
            .load()

        # Add metadata columns
        df = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_table", lit(p_source)) \
            .withColumn("_pipeline", lit(p_name))

        row_count = df.count()

        # Write as Parquet to MinIO (Bronze = raw dump, no table format needed)
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(p_target)

        elapsed = time.time() - t0
        total_rows += row_count

        print(f"     ✅ {row_count:,} rows in {elapsed:.1f}s")
        results.append({"table": p_name, "rows": row_count, "status": "success"})

    except Exception as e:
        print(f"     ❌ Error: {str(e)[:200]}")
        results.append({"table": p_name, "rows": 0, "status": f"error: {str(e)[:100]}"})


# =============================================================
# SUMMARY
# =============================================================

total_time = time.time() - start_time
print(f"\n{'='*60}")
print(f"  Bronze Ingestion Complete!")
print(f"  Tables: {len(results)}")
print(f"  Total Rows: {total_rows:,}")
print(f"  Time: {total_time:.1f}s")
print(f"{'='*60}")

for r in results:
    icon = "✅" if r["status"] == "success" else "❌"
    print(f"  {icon} {r['table']:15s} → {r['rows']:>10,} rows")

spark.stop()
