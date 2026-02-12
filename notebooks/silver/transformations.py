"""
Silver Layer - Standard PySpark Transformations
================================================
Reads Bronze Parquet tables from MinIO, applies transformations
and data quality rules defined in config/pipelines.yml,
writes Silver Iceberg tables to MinIO via the lakehouse catalog.

DQ rules from YAML → filter/validate rows:
  severity=critical/error → FAIL (halt pipeline)
  severity=warning        → DROP (filter bad rows)
  severity=info           → WARN (log only)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, row_number, expr
)
from pyspark.sql.window import Window
import sys
import os

# Add parent dir to path for shared module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _metadata import (
    get_pipelines, dq_rule_to_expectation,
    apply_transformations, apply_null_handling
)

# =============================================================
# SPARK SESSION
# =============================================================

spark = SparkSession.builder \
    .appName("Silver-Transformations-TPC-H") \
    .getOrCreate()

print("=" * 60)
print("Silver Layer — DataFrame Transformations (Iceberg)")
print("=" * 60)


# =============================================================
# LOAD CONFIG FROM YAML
# =============================================================

silver_pipelines = get_pipelines("silver")
print(f"\n📋 Active Silver pipelines: {len(silver_pipelines)}")


# =============================================================
# ENSURE ICEBERG NAMESPACES EXIST
# =============================================================

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
print("📁 Iceberg namespace: lakehouse.silver")


# =============================================================
# PROCESS EACH SILVER PIPELINE
# =============================================================

results = []

for pipeline in silver_pipelines:
    p_name = pipeline["name"]
    p_source = pipeline["source"]           # Bronze Parquet path
    p_primary_key = pipeline["primary_key"]
    p_scd_type = pipeline.get("scd_type", 1)
    p_target = pipeline.get("target_path", f"s3a://silver/{p_name}")

    # Iceberg table name in the lakehouse catalog
    iceberg_table = f"lakehouse.silver.{p_name}"

    # Get inline config from YAML
    transform_rules = pipeline.get("transformations", [])
    dq_rules = pipeline.get("data_quality", [])
    null_rules = pipeline.get("null_handling", [])

    print(f"\n{'─' * 50}")
    print(f"  Processing: {p_name}")
    print(f"  Source: {p_source}")
    print(f"  Iceberg table: {iceberg_table}")
    print(f"  DQ rules: {len(dq_rules)}, Transforms: {len(transform_rules)}")
    print(f"{'─' * 50}")

    try:
        # 1. Read from Bronze Parquet table
        df = spark.read.format("parquet").load(p_source)
        initial_count = df.count()
        print(f"  📥 Read {initial_count} rows from Bronze")

        # 2. Apply transformation rules from YAML
        if transform_rules:
            df = apply_transformations(df, transform_rules)
            print(f"  🔄 Applied {len(transform_rules)} transformation rules")

        # 3. Apply null handling
        if null_rules:
            df = apply_null_handling(df, null_rules)
            print(f"  🧹 Applied {len(null_rules)} null handling rules")

        # 4. Apply Data Quality rules
        dq_failed = False
        for rule in dq_rules:
            name, sql_expr, mode = dq_rule_to_expectation(rule)

            # Count rows that FAIL the DQ check
            bad_count = df.filter(f"NOT ({sql_expr})").count()

            if bad_count > 0:
                pct = (bad_count / initial_count * 100) if initial_count > 0 else 0
                print(f"  ⚠️  DQ [{name}]: {bad_count} rows fail ({pct:.1f}%) — mode={mode}")

                if mode == "FAIL":
                    print(f"  ❌ CRITICAL DQ failure on {name} — halting pipeline!")
                    dq_failed = True
                    break
                elif mode == "DROP":
                    df = df.filter(sql_expr)
                    print(f"  🗑️  Dropped {bad_count} rows for {name}")
                # mode == "WARN" → just log, keep rows
            else:
                print(f"  ✅ DQ [{name}]: passed")

        if dq_failed:
            results.append({"table": p_name, "status": "dq_failed"})
            continue

        # 5. Deduplicate by primary key (keep latest)
        pk_cols = [c.strip() for c in p_primary_key.split(",")] if p_primary_key else []
        if pk_cols and "_ingested_at" in df.columns:
            w = Window.partitionBy(*pk_cols).orderBy(
                col("_ingested_at").desc()
            )
            before = df.count()
            df = df.withColumn("_rn", row_number().over(w)) \
                   .filter(col("_rn") == 1) \
                   .drop("_rn")
            after = df.count()
            if before != after:
                print(f"  🔑 Deduplicated: {before} → {after} rows")

        # 6. Add Silver metadata
        df = df.withColumn("_silver_processed_at", current_timestamp()) \
               .withColumn("_pipeline", lit(p_name))

        # 7. SCD Type 2 tracking columns
        if p_scd_type == 2:
            df = df.withColumn("_valid_from", current_timestamp()) \
                   .withColumn("_valid_to", lit(None).cast("timestamp")) \
                   .withColumn("_is_current", lit(True))

        # 8. Write to Silver as Iceberg table
        final_count = df.count()
        df.writeTo(iceberg_table) \
            .using("iceberg") \
            .createOrReplace()

        print(f"  ✅ Written {final_count} rows to {iceberg_table}")
        results.append({
            "table": p_name,
            "status": "success",
            "rows_read": initial_count,
            "rows_written": final_count,
        })

    except Exception as e:
        print(f"  ❌ Error processing {p_name}: {e}")
        results.append({"table": p_name, "status": "error", "error": str(e)})


# =============================================================
# SUMMARY
# =============================================================

print(f"\n{'=' * 60}")
print(f"Silver Layer — Complete")
print(f"{'=' * 60}")
for r in results:
    status_icon = "✅" if r["status"] == "success" else "❌"
    print(f"  {status_icon} {r['table']}: {r['status']}")

# Fail if any critical errors
errors = [r for r in results if r["status"] in ("error", "dq_failed")]
if errors:
    print(f"\n❌ {len(errors)} tables failed!")
    sys.exit(1)

print(f"\n✅ All {len(results)} Silver tables processed successfully!")
spark.stop()
