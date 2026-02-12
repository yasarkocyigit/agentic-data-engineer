"""
Shared Metadata Client for SDP Pipeline
========================================
Reads pipeline config from YAML files (config-as-code).
Writes runtime data (audit, metrics) to PostgreSQL ai.* schema.

Config source: config/pipelines.yml, config/connections.yml
Runtime data:  PostgreSQL ai.* schema

IMPORTANT: SDP constraint - no collect(), count(), save() inside
@dp functions. All I/O calls happen here at module level.
"""

import yaml
import psycopg2
import json
import os
import re
from datetime import datetime
from pathlib import Path


# =============================================================
# CONFIGURATION
# =============================================================

# Find project root (where config/ directory lives)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_DIR = _PROJECT_ROOT / "config"

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "host.docker.internal"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "database": os.getenv("POSTGRES_DB", "controldb"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "changeme")
}


# =============================================================
# YAML CONFIG READER
# =============================================================

def _resolve_env_vars(value: str) -> str:
    """
    Resolve ${ENV_VAR:default} patterns in YAML values.
    Example: ${SQLSERVER_HOST:localhost} → os.getenv or 'localhost'
    """
    if not isinstance(value, str):
        return value

    def replacer(match):
        var_expr = match.group(1)
        if ":" in var_expr:
            var_name, default = var_expr.split(":", 1)
        else:
            var_name, default = var_expr, ""
        return os.getenv(var_name, default)

    return re.sub(r'\$\{([^}]+)\}', replacer, value)


def _resolve_dict(d):
    """Recursively resolve env vars in a dict/list structure."""
    if isinstance(d, dict):
        return {k: _resolve_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [_resolve_dict(item) for item in d]
    elif isinstance(d, str):
        return _resolve_env_vars(d)
    return d


def load_yaml(filename: str) -> dict:
    """Load and resolve a YAML config file."""
    filepath = _CONFIG_DIR / filename
    with open(filepath) as f:
        raw = yaml.safe_load(f)
    return _resolve_dict(raw)


# =============================================================
# PIPELINE CONFIG (from YAML)
# =============================================================

def get_pipelines(layer: str) -> list:
    """
    Get active pipelines for a layer from config/pipelines.yml.
    Returns list of pipeline dicts.
    """
    config = load_yaml("pipelines.yml")
    pipelines = config.get(layer, [])
    # Filter active only
    return [p for p in pipelines if p.get("is_active", True)]


def get_connection(connection_name: str) -> dict:
    """
    Get connection config from config/connections.yml.
    Returns dict with url, user, password, driver, options.
    """
    connections = load_yaml("connections.yml")
    return connections.get(connection_name, {})


def get_settings() -> dict:
    """Load config/settings.yml."""
    return load_yaml("settings.yml")


def get_feature_flags() -> dict:
    """Get feature flags from settings."""
    settings = get_settings()
    return settings.get("feature_flags", {})


# =============================================================
# JDBC HELPER
# =============================================================

def build_jdbc_url(connection_name: str) -> str:
    """Build JDBC URL from named connection in connections.yml."""
    conn = get_connection(connection_name)
    if conn.get("url"):
        return conn["url"]
    # Fallback
    host = os.getenv("SQLSERVER_HOST", "host.docker.internal")
    port = os.getenv("SQLSERVER_PORT", "1433")
    db = os.getenv("SQLSERVER_DB", "AdventureWorks2019")
    return f"jdbc:sqlserver://{host}:{port};databaseName={db};encrypt=false"


def build_jdbc_properties(connection_name: str) -> dict:
    """Build JDBC properties from named connection."""
    conn = get_connection(connection_name)
    props = {
        "user": conn.get("user", os.getenv("SQLSERVER_USER", "sa")),
        "password": conn.get("password", os.getenv("SQLSERVER_PASSWORD", "YourStr0ngP@ssw0rd")),
        "driver": conn.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    }
    # Add options (fetchsize, batchsize etc.)
    options = conn.get("options", {})
    for k, v in options.items():
        props[k] = str(v)
    return props


# =============================================================
# DQ RULE TO SDP EXPECTATION CONVERTER
# =============================================================

def dq_rule_to_expectation(rule: dict) -> tuple:
    """
    Convert a DQ rule dict (from YAML) to SDP expectation tuple.
    Returns (name, sql_expression, mode)

    Severity mapping:
      critical/error -> FAIL (stop pipeline)
      warning        -> DROP (filter bad rows)
      info           -> WARN (log only)
    """
    name = rule["name"]
    rtype = rule["type"]
    column = rule["column"]
    expression = rule.get("expression", "")
    severity = rule["severity"]

    # Map severity to SDP mode
    if severity in ("critical", "error"):
        mode = "FAIL"
    elif severity == "warning":
        mode = "DROP"
    else:
        mode = "WARN"

    # Build SQL boolean expression
    if rtype == "not_null":
        sql_expr = f"{column} IS NOT NULL"
    elif rtype == "unique":
        sql_expr = f"{column} IS NOT NULL"
        mode = "WARN"  # Row-level uniqueness is monitored, not enforced
    elif rtype == "range" and expression:
        parts = expression.split(",")
        conditions = []
        if len(parts) > 0 and parts[0].strip():
            conditions.append(f"{column} >= {parts[0].strip()}")
        if len(parts) > 1 and parts[1].strip():
            conditions.append(f"{column} <= {parts[1].strip()}")
        sql_expr = " AND ".join(conditions) if conditions else "1=1"
    elif rtype == "regex" and expression:
        sql_expr = f"{column} RLIKE '{expression}'"
    elif rtype == "custom_sql" and expression:
        sql_expr = f"NOT ({expression})"
    else:
        sql_expr = "1=1"

    return (name, sql_expr, mode)


# =============================================================
# TRANSFORMATION HELPER (Pure DataFrame transforms)
# =============================================================

def apply_transformations(df, rules: list):
    """
    Apply transformation rules from YAML config.
    Safe inside @dp functions (no side effects).
    """
    from pyspark.sql.functions import (
        col, expr, lit, when, trim, upper, lower, coalesce,
        concat_ws, sha2, regexp_replace, substring
    )
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType,
        FloatType, TimestampType, BooleanType, DateType
    )

    TYPE_MAP = {
        "string": StringType(), "str": StringType(),
        "int": IntegerType(), "integer": IntegerType(),
        "long": LongType(), "bigint": LongType(),
        "double": DoubleType(), "float": FloatType(),
        "timestamp": TimestampType(), "datetime": TimestampType(),
        "date": DateType(),
        "boolean": BooleanType(), "bool": BooleanType(),
    }

    for rule in rules:
        src = rule["source"]
        tgt = rule["target"]
        t = rule["type"]
        exp = rule.get("expression", "")

        if t == "rename":
            df = df.withColumnRenamed(src, tgt)
        elif t == "cast":
            df = df.withColumn(tgt, col(src).cast(
                TYPE_MAP.get(exp.lower(), StringType())))
        elif t == "derive":
            df = df.withColumn(tgt, expr(exp))
        elif t == "mask":
            n = int(exp) if exp and exp.isdigit() else 0
            if n > 0:
                df = df.withColumn(tgt, concat_ws(
                    "", lit("*" * 10), substring(col(src).cast("string"), -n, n)))
            else:
                df = df.withColumn(tgt, lit("****"))
        elif t == "hash":
            df = df.withColumn(tgt, sha2(col(src).cast("string"), 256))
        elif t == "trim":
            df = df.withColumn(tgt, trim(col(src)))
        elif t == "upper":
            df = df.withColumn(tgt, upper(col(src)))
        elif t == "lower":
            df = df.withColumn(tgt, lower(col(src)))
        elif t == "coalesce":
            cols = [col(c.strip()) for c in exp.split(",")]
            df = df.withColumn(tgt, coalesce(*cols))
        elif t == "default_value":
            df = df.withColumn(tgt, when(
                col(src).isNull(), lit(exp)).otherwise(col(src)))
        elif t == "regex_replace" and "|||" in exp:
            parts = exp.split("|||")
            df = df.withColumn(tgt, regexp_replace(
                col(src), parts[0], parts[1]))
        elif t == "concat":
            cols = [col(c.strip()) for c in exp.split(",")]
            df = df.withColumn(tgt, concat_ws("_", *cols))

    return df


def apply_null_handling(df, rules: list):
    """Apply null handling from config. Safe inside @dp functions."""
    from pyspark.sql.functions import col, lit, when, coalesce

    for rule in rules:
        column = rule["column"]
        strategy = rule["strategy"]
        default = rule.get("default_value")
        coalesce_cols = rule.get("coalesce_columns")

        if column not in df.columns:
            continue

        if strategy == "default_value" and default is not None:
            df = df.withColumn(column, when(
                col(column).isNull(), lit(default)
            ).otherwise(col(column)))
        elif strategy == "coalesce" and coalesce_cols:
            fallback = [col(c) for c in coalesce_cols if c in df.columns]
            if fallback:
                df = df.withColumn(column, coalesce(col(column), *fallback))
        elif strategy == "drop_row":
            df = df.filter(col(column).isNotNull())

    return df


# =============================================================
# AI RUNTIME CLIENT (PostgreSQL - ai.* schema only)
# =============================================================

class AIClient:
    """PostgreSQL client for AI runtime data only (ai.* schema)."""

    def __init__(self, config: dict = None):
        self._config = config or POSTGRES_CONFIG

    def _conn(self):
        return psycopg2.connect(**self._config)

    def log_audit(self, pipeline_id, action: str, details: dict):
        """Log action to ai.audit_logs"""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai.audit_logs
                (entity_type, entity_id, action, actor, details)
            VALUES ('pipeline', %s, %s, 'spark-sdp', %s)
        """, (str(pipeline_id), action, json.dumps(details)))
        conn.commit()
        cur.close()
        conn.close()

    def log_execution_metric(self, pipeline_name: str, run_id: str,
                             status: str, rows_read: int = 0,
                             rows_written: int = 0,
                             start_time=None, end_time=None,
                             error: str = None):
        """Log pipeline execution metrics"""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai.execution_metrics
                (run_id, status, rows_read, rows_written,
                 start_time, end_time, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, status, rows_read, rows_written,
              start_time or datetime.now(), end_time, error))
        conn.commit()
        cur.close()
        conn.close()
