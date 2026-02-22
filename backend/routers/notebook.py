"""
Notebook Router — Interactive code execution via Jupyter Kernel Gateway.
Provides kernel session management and cell execution for the Notebook UI.

Falls back to a local in-memory execution engine when Kernel Gateway is unavailable.
"""

import asyncio
import ast
import contextlib
import glob
import importlib
import io
import json
import logging
import os
import re
import threading
import traceback
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

KERNEL_GW_URL = os.getenv("JUPYTER_KERNEL_GW_URL", "http://localhost:8888")
NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "notebooks"))

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8083"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "openclaw-notebook")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
SPARK_DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
SPARK_DEFAULT_DATABASE = os.getenv("SPARK_DEFAULT_DATABASE", "default")
SPARK_S3_ENDPOINT = os.getenv("SPARK_S3_ENDPOINT", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
SPARK_S3_ACCESS_KEY = os.getenv("SPARK_S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
SPARK_S3_SECRET_KEY = os.getenv("SPARK_S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "admin123"))
SPARK_S3_PATH_STYLE_ACCESS = (os.getenv("SPARK_S3_PATH_STYLE_ACCESS", "true") or "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SPARK_EXTRA_JARS = os.getenv(
    "SPARK_EXTRA_JARS",
    "/workspace/jars/hadoop-aws-3.4.1.jar,/workspace/jars/bundle-2.31.1.jar",
)
SPARK_DRIVER_JARS_DIR = os.getenv("SPARK_DRIVER_JARS_DIR", "/workspace/jars")
SPARK_EXECUTOR_JARS_DIR = os.getenv("SPARK_EXECUTOR_JARS_DIR", "/opt/spark/extra-jars")
SPARK_DELTA_ENABLED = (os.getenv("SPARK_DELTA_ENABLED", "true") or "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SPARK_DELTA_PACKAGE = os.getenv("SPARK_DELTA_PACKAGE", "io.delta:delta-spark_2.13:4.0.1")
SPARK_EXTRA_PACKAGES = os.getenv("SPARK_EXTRA_PACKAGES", "")
SPARK_JARS_REPOSITORIES = os.getenv("SPARK_JARS_REPOSITORIES", "")
NOTEBOOK_SQL_ENGINE = (os.getenv("NOTEBOOK_SQL_ENGINE", "spark") or "spark").strip().lower()
if NOTEBOOK_SQL_ENGINE not in {"spark", "trino"}:
    NOTEBOOK_SQL_ENGINE = "spark"
NOTEBOOK_USE_GATEWAY = (os.getenv("NOTEBOOK_USE_GATEWAY", "false") or "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

try:
    NOTEBOOK_SESSION_TTL_SECONDS = max(300, int(os.getenv("NOTEBOOK_SESSION_TTL_SECONDS", "7200")))
except ValueError:
    NOTEBOOK_SESSION_TTL_SECONDS = 7200

try:
    NOTEBOOK_TABLE_ROW_LIMIT = max(50, int(os.getenv("NOTEBOOK_TABLE_ROW_LIMIT", "500")))
except ValueError:
    NOTEBOOK_TABLE_ROW_LIMIT = 500

# Ensure notebooks directory exists
Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)


# ─── Models ───

class ExecuteRequest(BaseModel):
    code: str
    kernel_id: Optional[str] = None


class SaveNotebookRequest(BaseModel):
    name: str
    cells: list[dict]


class RenameNotebookRequest(BaseModel):
    old_name: str
    new_name: str


# ─── In-Memory Session Store ───
# session_id -> { kernel_id, created_at, last_used, mode }
_sessions: dict[str, dict[str, Any]] = {}
_execution_counts: dict[str, int] = {}
_local_namespaces: dict[str, dict[str, Any]] = {}
_session_locks: dict[str, asyncio.Lock] = {}
_spark_session: Any = None
_spark_session_lock = threading.Lock()
_spark_query_lock = threading.Lock()


# ─── Helpers ───


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _sanitize_notebook_name(value: str) -> str:
    return "".join(c for c in str(value) if c.isalnum() or c in "-_ ").strip()


def _require_valid_notebook_name(name: str) -> str:
    original = str(name).strip()
    safe = _sanitize_notebook_name(original)
    if not safe or safe != original:
        raise HTTPException(status_code=400, detail="Invalid notebook name")
    return safe


def _touch_session(session_id: str) -> None:
    session = _sessions.get(session_id)
    if session:
        session["last_used"] = _now_iso()


async def _cleanup_stale_sessions() -> int:
    """Remove stale local/gateway sessions to avoid kernel leaks."""
    now = _now_utc()
    stale_ids: list[str] = []

    for sid, info in list(_sessions.items()):
        last_seen = _parse_iso(info.get("last_used")) or _parse_iso(info.get("created_at"))
        if last_seen is None:
            stale_ids.append(sid)
            continue
        age = (now - last_seen).total_seconds()
        if age > NOTEBOOK_SESSION_TTL_SECONDS:
            stale_ids.append(sid)

    for sid in stale_ids:
        info = _sessions.pop(sid, None)
        _execution_counts.pop(sid, None)
        _local_namespaces.pop(sid, None)
        _session_locks.pop(sid, None)
        if info and info.get("mode") == "gateway" and info.get("kernel_id"):
            try:
                await _kg_request("DELETE", f"/api/kernels/{info['kernel_id']}")
            except Exception:
                pass

    return len(stale_ids)


async def _kernel_gw_available() -> bool:
    """Check if Jupyter Kernel Gateway is reachable."""
    if not NOTEBOOK_USE_GATEWAY:
        return False
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{KERNEL_GW_URL}/api")
            return resp.status_code == 200
    except Exception:
        return False


async def _kg_request(method: str, path: str, **kwargs) -> dict:
    """Make a request to the Kernel Gateway API."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(method, f"{KERNEL_GW_URL}{path}", **kwargs)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json() if resp.text else {}


def _kernel_ws_url(kernel_id: str) -> str:
    parsed = urlparse(KERNEL_GW_URL)
    if not parsed.netloc:
        parsed = urlparse(f"http://{KERNEL_GW_URL}")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}/api/kernels/{kernel_id}/channels"


async def _execute_via_gateway(kernel_id: str, code: str) -> dict:
    """Execute code via Kernel Gateway WebSocket protocol."""
    import websockets  # type: ignore

    ws_url = _kernel_ws_url(kernel_id)

    msg_id = str(uuid.uuid4())
    execute_msg = {
        "header": {
            "msg_id": msg_id,
            "msg_type": "execute_request",
            "username": "openclaw",
            "session": str(uuid.uuid4()),
            "date": _now_iso(),
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
            "stop_on_error": True,
        },
        "buffers": [],
        "channel": "shell",
    }

    outputs: list[dict[str, Any]] = []
    status = "ok"
    execution_count = None

    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps(execute_msg))

            deadline = _now_utc().timestamp() + 60
            while _now_utc().timestamp() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    msg = json.loads(raw)
                except asyncio.TimeoutError:
                    break

                msg_type = msg.get("msg_type", "")
                parent_id = msg.get("parent_header", {}).get("msg_id", "")

                if parent_id != msg_id:
                    continue

                if msg_type == "stream":
                    outputs.append({
                        "output_type": "stream",
                        "name": msg["content"].get("name", "stdout"),
                        "text": msg["content"].get("text", ""),
                    })
                elif msg_type == "execute_result":
                    execution_count = msg["content"].get("execution_count")
                    data = msg["content"].get("data", {})
                    outputs.append({
                        "output_type": "execute_result",
                        "data": data,
                        "execution_count": execution_count,
                    })
                elif msg_type == "display_data":
                    outputs.append({
                        "output_type": "display_data",
                        "data": msg["content"].get("data", {}),
                    })
                elif msg_type == "error":
                    status = "error"
                    outputs.append({
                        "output_type": "error",
                        "ename": msg["content"].get("ename", "Error"),
                        "evalue": msg["content"].get("evalue", ""),
                        "traceback": msg["content"].get("traceback", []),
                    })
                elif msg_type == "execute_reply":
                    status = msg["content"].get("status", "ok")
                    execution_count = msg["content"].get("execution_count")
                    break

    except Exception as e:
        logger.error("WebSocket execution error: %s", e)
        status = "error"
        outputs.append({
            "output_type": "error",
            "ename": "ConnectionError",
            "evalue": str(e),
            "traceback": [],
        })

    return {
        "status": status,
        "execution_count": execution_count,
        "outputs": outputs,
    }


def _strip_sql_line_comment(line: str) -> str:
    """Remove -- comments while preserving quoted strings."""
    result: list[str] = []
    i = 0
    in_single = False
    in_double = False

    while i < len(line):
        ch = line[i]
        nxt = line[i + 1] if i + 1 < len(line) else ""

        if in_single:
            result.append(ch)
            if ch == "'":
                if nxt == "'":
                    result.append(nxt)
                    i += 2
                    continue
                in_single = False
            i += 1
            continue

        if in_double:
            result.append(ch)
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "'":
            in_single = True
            result.append(ch)
            i += 1
            continue

        if ch == '"':
            in_double = True
            result.append(ch)
            i += 1
            continue

        if ch == "-" and nxt == "-":
            break

        result.append(ch)
        i += 1

    return "".join(result)


def _sanitize_sql(raw_sql: str) -> str:
    """Clean SQL for Trino: strip comments, trailing semicolons, and empty lines."""
    cleaned_lines: list[str] = []
    for line in raw_sql.split("\n"):
        no_comment = _strip_sql_line_comment(line).rstrip()
        if no_comment:
            cleaned_lines.append(no_comment)

    sql = "\n".join(cleaned_lines).strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _discover_spark_jars() -> list[str]:
    if not SPARK_EXTRA_JARS:
        return []

    seen: set[str] = set()
    discovered: list[str] = []

    for token in [item.strip() for item in SPARK_EXTRA_JARS.split(",") if item.strip()]:
        candidates = glob.glob(token) if any(ch in token for ch in "*?[]") else [token]
        for path in candidates:
            if os.path.isfile(path) and path not in seen:
                seen.add(path)
                discovered.append(path)

    return sorted(discovered)


def _build_spark_classpath(jars: list[str], base_dir: str) -> str:
    if not jars:
        return ""
    mapped = [os.path.join(base_dir, os.path.basename(jar)) for jar in jars]
    return ":".join(mapped)


def _spark_session_usable(session: Any) -> bool:
    if session is None:
        return False
    try:
        spark_context = session.sparkContext
        jsc = getattr(spark_context, "_jsc", None)
        if jsc is None:
            return False
        return not bool(jsc.sc().isStopped())
    except Exception:
        return False


def _reset_spark_singletons() -> None:
    try:
        spark_context_mod = importlib.import_module("pyspark")
        if hasattr(spark_context_mod, "SparkContext"):
            spark_context_mod.SparkContext._active_spark_context = None
    except Exception:
        pass

    try:
        spark_sql_mod = importlib.import_module("pyspark.sql")
        spark_session_cls = spark_sql_mod.SparkSession
        if hasattr(spark_session_cls, "_instantiatedSession"):
            spark_session_cls._instantiatedSession = None
        if hasattr(spark_session_cls, "_activeSession"):
            spark_session_cls._activeSession = None
    except Exception:
        pass


def _get_spark_session():
    global _spark_session
    if _spark_session_usable(_spark_session):
        return _spark_session
    _spark_session = None

    with _spark_session_lock:
        if _spark_session_usable(_spark_session):
            return _spark_session
        _spark_session = None
        _reset_spark_singletons()

        spark_sql = importlib.import_module("pyspark.sql")
        SparkSession = spark_sql.SparkSession
        builder = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(SPARK_MASTER_URL)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", str(SPARK_S3_PATH_STYLE_ACCESS).lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", SPARK_S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SPARK_S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(SPARK_S3_ENDPOINT.startswith("https://")).lower())
        )
        if SPARK_DRIVER_HOST:
            builder = builder.config("spark.driver.host", SPARK_DRIVER_HOST)
        if SPARK_DRIVER_BIND_ADDRESS:
            builder = builder.config("spark.driver.bindAddress", SPARK_DRIVER_BIND_ADDRESS)
        local_jars = _discover_spark_jars()
        if local_jars:
            builder = builder.config("spark.jars", ",".join(local_jars))
            driver_cp = _build_spark_classpath(local_jars, SPARK_DRIVER_JARS_DIR)
            executor_cp = _build_spark_classpath(local_jars, SPARK_EXECUTOR_JARS_DIR)
            if driver_cp:
                builder = builder.config("spark.driver.extraClassPath", driver_cp)
            if executor_cp:
                builder = builder.config("spark.executor.extraClassPath", executor_cp)

        extra_packages = [pkg.strip() for pkg in SPARK_EXTRA_PACKAGES.split(",") if pkg.strip()]
        if SPARK_DELTA_ENABLED and SPARK_DELTA_PACKAGE.strip():
            extra_packages.insert(0, SPARK_DELTA_PACKAGE.strip())
            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
        if SPARK_JARS_REPOSITORIES.strip():
            builder = builder.config("spark.jars.repositories", SPARK_JARS_REPOSITORIES.strip())

        if extra_packages:
            # Keep dependency resolution explicit to avoid version conflicts.
            ordered_unique_packages = list(dict.fromkeys(extra_packages))
            builder = builder.config("spark.jars.packages", ",".join(ordered_unique_packages))

        try:
            _spark_session = builder.getOrCreate()
        except Exception as exc:
            if "stopped SparkContext" not in str(exc):
                raise
            logger.warning("Detected stopped SparkContext. Resetting and recreating Spark session.")
            _reset_spark_singletons()
            _spark_session = builder.getOrCreate()
        logger.info("Notebook Spark session initialized. master=%s app=%s", SPARK_MASTER_URL, SPARK_APP_NAME)
        return _spark_session


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _table_output(columns: list[str], rows: list[list[Any]], execution_count: int) -> dict[str, Any]:
    return {
        "output_type": "execute_result",
        "data": {
            "application/json": {
                "columns": [str(c) for c in columns],
                "rows": [[_json_safe(v) for v in row] for row in rows],
            }
        },
        "execution_count": execution_count,
    }


def _format_value_output(value: Any, execution_count: int) -> Optional[dict[str, Any]]:
    if value is None:
        return None

    try:
        pd = importlib.import_module("pandas")
        if isinstance(value, pd.DataFrame):
            sample = value.head(NOTEBOOK_TABLE_ROW_LIMIT)
            columns = [str(c) for c in sample.columns]
            rows = sample.where(~sample.isna(), None).values.tolist()
            return _table_output(columns, rows, execution_count)
    except Exception:
        pass

    try:
        spark_sql = importlib.import_module("pyspark.sql")
        if isinstance(value, spark_sql.DataFrame):
            sample = value.limit(NOTEBOOK_TABLE_ROW_LIMIT)
            columns = [str(c) for c in sample.columns]
            rows = sample.collect()
            values = [[_json_safe(row[col]) for col in columns] for row in rows]
            return _table_output(columns, values, execution_count)
    except Exception:
        pass

    if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
        columns: list[str] = []
        seen: set[str] = set()
        for row_obj in value:
            for key in row_obj.keys():
                k = str(key)
                if k not in seen:
                    seen.add(k)
                    columns.append(k)
        rows = [[_json_safe(row_obj.get(col)) for col in columns] for row_obj in value[:NOTEBOOK_TABLE_ROW_LIMIT]]
        return _table_output(columns, rows, execution_count)

    safe = _json_safe(value)
    if isinstance(safe, (dict, list)):
        text = json.dumps(safe, ensure_ascii=False, indent=2)
    else:
        text = str(safe)

    return {
        "output_type": "execute_result",
        "data": {
            "text/plain": text,
        },
        "execution_count": execution_count,
    }


def _ensure_local_session(session_id: str) -> None:
    if session_id not in _sessions:
        now = _now_iso()
        _sessions[session_id] = {
            "kernel_id": session_id,
            "created_at": now,
            "last_used": now,
            "mode": "local",
        }
    if session_id not in _execution_counts:
        _execution_counts[session_id] = 0
    if session_id not in _local_namespaces:
        _local_namespaces[session_id] = {
            "__name__": "__main__",
            "__builtins__": __builtins__,
        }


def _install_local_helpers(namespace: dict[str, Any]) -> None:
    if namespace.get("__openclaw_helpers_loaded__"):
        return

    def _require_module(module_name: str, package_name: Optional[str] = None):
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            pkg = package_name or module_name.split(".")[0]
            raise RuntimeError(
                f"Optional dependency '{pkg}' is not installed. Install it to use this feature."
            ) from exc

    def query_trino(sql: str, catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA):
        trino_db = _require_module("trino.dbapi", "trino")
        conn = trino_db.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=catalog, schema=schema)
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description:
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        else:
            rows = []
            cols = []
        conn.close()
        return cols, rows

    def query_spark(
        statement: str,
        database: str = SPARK_DEFAULT_DATABASE,
        limit: Optional[int] = NOTEBOOK_TABLE_ROW_LIMIT,
    ):
        _require_module("pyspark.sql", "pyspark")
        spark = _get_spark_session()
        sql_text = _sanitize_sql(statement)
        if not sql_text:
            raise RuntimeError("Spark SQL statement is empty")

        with _spark_query_lock:
            target_db = str(database).strip() or SPARK_DEFAULT_DATABASE
            spark.catalog.setCurrentDatabase(target_db)
            df = spark.sql(sql_text)
            if limit is not None and int(limit) > 0:
                df = df.limit(int(limit))
            return df

    def query(
        statement: str,
        catalog: str = TRINO_CATALOG,
        schema: str = TRINO_SCHEMA,
        *,
        engine: str = NOTEBOOK_SQL_ENGINE,
        database: Optional[str] = None,
    ):
        selected_engine = (engine or NOTEBOOK_SQL_ENGINE).strip().lower()
        if selected_engine == "spark":
            return query_spark(statement, database=database or schema or SPARK_DEFAULT_DATABASE)
        if selected_engine != "trino":
            raise RuntimeError(f"Unsupported SQL engine: {selected_engine}")

        pd = _require_module("pandas", "pandas")
        cols, rows = query_trino(statement, catalog=catalog, schema=schema)
        return pd.DataFrame(rows, columns=cols)

    def sql(
        statement: str,
        catalog: str = TRINO_CATALOG,
        schema: str = TRINO_SCHEMA,
        *,
        engine: str = NOTEBOOK_SQL_ENGINE,
        database: Optional[str] = None,
    ):
        return query(statement, catalog=catalog, schema=schema, engine=engine, database=database)

    def spark_sql(
        statement: str,
        database: str = SPARK_DEFAULT_DATABASE,
        limit: Optional[int] = NOTEBOOK_TABLE_ROW_LIMIT,
    ):
        return query_spark(statement, database=database, limit=limit)

    spark_sql_module = _require_module("pyspark.sql", "pyspark")
    spark_functions = _require_module("pyspark.sql.functions", "pyspark")
    spark_types = _require_module("pyspark.sql.types", "pyspark")
    spark = _get_spark_session()

    namespace["query_trino"] = query_trino
    namespace["query_spark"] = query_spark
    namespace["query"] = query
    namespace["sql"] = sql
    namespace["spark_sql"] = spark_sql
    namespace["spark"] = spark
    namespace["SparkSession"] = spark_sql_module.SparkSession
    namespace["F"] = spark_functions
    namespace["T"] = spark_types
    namespace["__openclaw_sql_default_engine__"] = NOTEBOOK_SQL_ENGINE
    namespace["__openclaw_helpers_loaded__"] = True


def _execute_trino_sql(raw_sql: str, execution_count: int) -> dict[str, Any]:
    outputs: list[dict[str, Any]] = []
    sql = _sanitize_sql(raw_sql)
    if not sql:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "ValueError",
                "evalue": "SQL cell is empty after sanitization",
                "traceback": [],
            }],
        }

    try:
        trino_db = importlib.import_module("trino.dbapi")
    except ModuleNotFoundError:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "DependencyError",
                "evalue": "Optional dependency 'trino' is not installed. Install it to run %%sql cells.",
                "traceback": [],
            }],
        }

    try:
        conn = trino_db.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )
        cur = conn.cursor()
        cur.execute(sql)

        if cur.description:
            rows = cur.fetchmany(NOTEBOOK_TABLE_ROW_LIMIT + 1)
            columns = [d[0] for d in cur.description]
            truncated = len(rows) > NOTEBOOK_TABLE_ROW_LIMIT
            rows = rows[:NOTEBOOK_TABLE_ROW_LIMIT]
            outputs.append(_table_output(columns, [list(r) for r in rows], execution_count))
            if truncated:
                outputs.append({
                    "output_type": "stream",
                    "name": "stdout",
                    "text": f"Result truncated to {NOTEBOOK_TABLE_ROW_LIMIT} rows.\\n",
                })
        else:
            outputs.append({
                "output_type": "stream",
                "name": "stdout",
                "text": "Statement executed successfully.\\n",
            })

        conn.close()
        return {
            "status": "ok",
            "execution_count": execution_count,
            "outputs": outputs,
        }

    except Exception as exc:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": type(exc).__name__,
                "evalue": str(exc),
                "traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
            }],
        }


def _execute_spark_sql(raw_sql: str, execution_count: int, database: str = SPARK_DEFAULT_DATABASE) -> dict[str, Any]:
    outputs: list[dict[str, Any]] = []
    sql = _sanitize_sql(raw_sql)
    if not sql:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "ValueError",
                "evalue": "SQL cell is empty after sanitization",
                "traceback": [],
            }],
        }

    try:
        importlib.import_module("pyspark.sql")
    except ModuleNotFoundError:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "DependencyError",
                "evalue": "Optional dependency 'pyspark' is not installed. Install it to run Spark SQL cells.",
                "traceback": [],
            }],
        }

    try:
        spark = _get_spark_session()
        with _spark_query_lock:
            target_db = str(database).strip() or SPARK_DEFAULT_DATABASE
            spark.catalog.setCurrentDatabase(target_db)
            df = spark.sql(sql)
            columns = [str(c) for c in df.columns]

            if columns:
                rows = df.limit(NOTEBOOK_TABLE_ROW_LIMIT + 1).collect()
                truncated = len(rows) > NOTEBOOK_TABLE_ROW_LIMIT
                rows = rows[:NOTEBOOK_TABLE_ROW_LIMIT]
                values = [[row[col] for col in columns] for row in rows]
                outputs.append(_table_output(columns, values, execution_count))
                if truncated:
                    outputs.append({
                        "output_type": "stream",
                        "name": "stdout",
                        "text": f"Result truncated to {NOTEBOOK_TABLE_ROW_LIMIT} rows.\\n",
                    })
            else:
                outputs.append({
                    "output_type": "stream",
                    "name": "stdout",
                    "text": "Statement executed successfully.\\n",
                })

        return {
            "status": "ok",
            "execution_count": execution_count,
            "outputs": outputs,
        }

    except Exception as exc:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": type(exc).__name__,
                "evalue": str(exc),
                "traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
            }],
        }


def _compile_cell(code: str) -> tuple[Optional[Any], Optional[Any]]:
    parsed = ast.parse(code, mode="exec")
    body = list(parsed.body)

    if body and isinstance(body[-1], ast.Expr):
        expr = body.pop()
        module = ast.Module(body=body, type_ignores=[])
        ast.fix_missing_locations(module)
        exec_code = compile(module, "<cell>", "exec") if body else None
        expr_code = compile(ast.Expression(expr.value), "<cell>", "eval")
        return exec_code, expr_code

    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)
    exec_code = compile(module, "<cell>", "exec") if body else None
    return exec_code, None


def _execute_local_python(code: str, session_id: str, execution_count: int) -> dict[str, Any]:
    _ensure_local_session(session_id)
    namespace = _local_namespaces[session_id]
    try:
        _install_local_helpers(namespace)
    except Exception as exc:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": type(exc).__name__,
                "evalue": str(exc),
                "traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
            }],
        }

    outputs: list[dict[str, Any]] = []
    display_outputs: list[dict[str, Any]] = []

    def display(value: Any, _n: int = 20) -> None:
        rendered = _format_value_output(value, execution_count)
        if rendered:
            display_outputs.append(rendered)

    namespace["display"] = display

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    try:
        exec_code, expr_code = _compile_cell(code)
    except Exception as exc:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": type(exc).__name__,
                "evalue": str(exc),
                "traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
            }],
        }

    status = "ok"
    expr_value = None

    try:
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            if exec_code is not None:
                exec(exec_code, namespace, namespace)
            if expr_code is not None:
                expr_value = eval(expr_code, namespace, namespace)
    except Exception as exc:
        status = "error"
        outputs.append({
            "output_type": "error",
            "ename": type(exc).__name__,
            "evalue": str(exc),
            "traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
        })

    stdout_text = stdout_buffer.getvalue()
    stderr_text = stderr_buffer.getvalue()

    if stdout_text:
        outputs.insert(0, {"output_type": "stream", "name": "stdout", "text": stdout_text})
    if stderr_text:
        outputs.insert(0, {"output_type": "stream", "name": "stderr", "text": stderr_text})

    outputs.extend(display_outputs)

    if status == "ok" and expr_value is not None:
        rendered = _format_value_output(expr_value, execution_count)
        if rendered:
            outputs.append(rendered)

    return {
        "status": status,
        "execution_count": execution_count,
        "outputs": outputs,
    }


def _execute_local(code: str, session_id: str) -> dict[str, Any]:
    """Execute cell using local in-memory namespace (stateful across cells)."""
    _ensure_local_session(session_id)
    _touch_session(session_id)

    _execution_counts.setdefault(session_id, 0)
    _execution_counts[session_id] += 1
    execution_count = _execution_counts[session_id]

    stripped = (code or "").strip()
    if stripped.startswith("%%spark"):
        sql_code = stripped[len("%%spark"):].strip()
        return _execute_spark_sql(sql_code, execution_count)

    if stripped.startswith("%%trino"):
        sql_code = stripped[len("%%trino"):].strip()
        return _execute_trino_sql(sql_code, execution_count)

    if stripped.startswith("%%sql"):
        sql_body = stripped[len("%%sql"):].strip()
        selected_engine = NOTEBOOK_SQL_ENGINE
        match = re.match(r"^(spark|trino)\b", sql_body, re.IGNORECASE)
        if match:
            selected_engine = match.group(1).lower()
            sql_body = sql_body[len(match.group(0)):].strip()

        if selected_engine == "spark":
            return _execute_spark_sql(sql_body, execution_count)
        return _execute_trino_sql(sql_body, execution_count)

    return _execute_local_python(code, session_id, execution_count)


# ─── Routes: Kernel Management ───

@router.get("/notebook/kernels")
async def list_kernels():
    """List active kernel sessions."""
    await _cleanup_stale_sessions()

    gw_available = await _kernel_gw_available()
    if gw_available:
        try:
            kernels = await _kg_request("GET", "/api/kernels")
            return {"kernels": kernels, "mode": "gateway"}
        except Exception:
            pass

    return {
        "kernels": [
            {"id": sid, "name": "python3", "execution_state": "idle", **info}
            for sid, info in _sessions.items()
        ],
        "mode": "local",
    }


@router.post("/notebook/kernels")
async def start_kernel():
    """Start a new kernel session."""
    await _cleanup_stale_sessions()
    gw_available = await _kernel_gw_available()

    if gw_available:
        try:
            result = await _kg_request("POST", "/api/kernels", json={"name": "python3"})
            session_id = result.get("id", str(uuid.uuid4()))
            now = _now_iso()
            _sessions[session_id] = {
                "kernel_id": result.get("id"),
                "created_at": now,
                "last_used": now,
                "mode": "gateway",
            }
            return {"id": session_id, "mode": "gateway", "status": "started"}
        except Exception as e:
            logger.warning("Kernel Gateway unavailable, falling back to local: %s", e)

    session_id = str(uuid.uuid4())
    now = _now_iso()
    _sessions[session_id] = {
        "kernel_id": session_id,
        "created_at": now,
        "last_used": now,
        "mode": "local",
    }
    _execution_counts[session_id] = 0
    _local_namespaces[session_id] = {
        "__name__": "__main__",
        "__builtins__": __builtins__,
    }
    return {"id": session_id, "mode": "local", "status": "started"}


@router.delete("/notebook/kernels/{kernel_id}")
async def shutdown_kernel(kernel_id: str):
    """Shutdown a kernel session."""
    session = _sessions.pop(kernel_id, None)
    _execution_counts.pop(kernel_id, None)
    _local_namespaces.pop(kernel_id, None)
    _session_locks.pop(kernel_id, None)

    if session and session.get("mode") == "gateway" and session.get("kernel_id"):
        try:
            await _kg_request("DELETE", f"/api/kernels/{session['kernel_id']}")
        except Exception:
            pass

    return {"status": "shutdown", "id": kernel_id}


@router.post("/notebook/kernels/{kernel_id}/restart")
async def restart_kernel(kernel_id: str):
    """Restart a kernel session."""
    session = _sessions.get(kernel_id)
    if session and session.get("mode") == "gateway":
        try:
            await _kg_request("POST", f"/api/kernels/{session['kernel_id']}/restart")
            _touch_session(kernel_id)
            return {"status": "restarted", "mode": "gateway"}
        except Exception:
            pass

    _ensure_local_session(kernel_id)
    _execution_counts[kernel_id] = 0
    _local_namespaces[kernel_id] = {
        "__name__": "__main__",
        "__builtins__": __builtins__,
    }
    _touch_session(kernel_id)
    return {"status": "restarted", "mode": "local"}


@router.post("/notebook/kernels/{kernel_id}/interrupt")
async def interrupt_kernel(kernel_id: str):
    """Interrupt a running execution."""
    session = _sessions.get(kernel_id)
    if session and session.get("mode") == "gateway":
        try:
            await _kg_request("POST", f"/api/kernels/{session['kernel_id']}/interrupt")
            _touch_session(kernel_id)
            return {"status": "interrupted"}
        except Exception:
            pass
    _touch_session(kernel_id)
    return {"status": "interrupted"}


# ─── Routes: Cell Execution ───

@router.post("/notebook/kernels/{kernel_id}/execute")
async def execute_cell(kernel_id: str, req: ExecuteRequest):
    """Execute a code cell and return outputs."""
    await _cleanup_stale_sessions()
    session = _sessions.get(kernel_id)

    if session and session.get("mode") == "gateway":
        try:
            _touch_session(kernel_id)
            result = await _execute_via_gateway(session["kernel_id"], req.code)
            return result
        except Exception as e:
            logger.warning("Gateway execution failed, falling back to local: %s", e)

    _ensure_local_session(kernel_id)
    lock = _session_locks.setdefault(kernel_id, asyncio.Lock())
    async with lock:
        result = await asyncio.to_thread(_execute_local, req.code, kernel_id)
    _touch_session(kernel_id)
    return result


# ─── Routes: Notebook File Management ───

@router.get("/notebook/files")
async def list_notebooks():
    """List saved notebook files, deduplicated by notebook name."""
    entries: dict[str, dict[str, Any]] = {}
    nb_dir = Path(NOTEBOOKS_DIR)

    def add_entry(name: str, filename: str, size: int, modified: float, priority: int) -> None:
        current = entries.get(name)
        if (
            current is None
            or priority > current["_priority"]
            or (priority == current["_priority"] and modified > current["_modified_ts"])
        ):
            entries[name] = {
                "name": name,
                "filename": filename,
                "size": size,
                "modified": datetime.fromtimestamp(modified, timezone.utc).isoformat(),
                "_priority": priority,
                "_modified_ts": modified,
            }

    for f in sorted(nb_dir.glob("*.ipynb")):
        stat = f.stat()
        add_entry(f.stem, f.name, stat.st_size, stat.st_mtime, priority=1)

    for f in sorted(nb_dir.glob("*.notebook.json")):
        stat = f.stat()
        name = f.name.replace(".notebook.json", "")
        add_entry(name, f.name, stat.st_size, stat.st_mtime, priority=2)

    notebooks = sorted(
        [
            {
                "name": v["name"],
                "filename": v["filename"],
                "size": v["size"],
                "modified": v["modified"],
            }
            for v in entries.values()
        ],
        key=lambda item: item["name"].lower(),
    )

    return {"notebooks": notebooks}


@router.post("/notebook/files")
async def save_notebook(req: SaveNotebookRequest):
    """Save a notebook (cells + outputs)."""
    safe_name = _sanitize_notebook_name(req.name)
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid notebook name")

    filepath = Path(NOTEBOOKS_DIR) / f"{safe_name}.notebook.json"
    data = {
        "name": safe_name,
        "updated": _now_iso(),
        "cells": req.cells,
    }
    filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {"status": "saved", "filename": filepath.name, "name": safe_name}


@router.post("/notebook/files/rename")
async def rename_notebook(req: RenameNotebookRequest):
    """Rename notebook files for both .notebook.json and .ipynb variants."""
    old_name = _require_valid_notebook_name(req.old_name)
    new_name = _sanitize_notebook_name(req.new_name)

    if not new_name:
        raise HTTPException(status_code=400, detail="Invalid notebook name")
    if old_name == new_name:
        return {"status": "unchanged", "name": new_name, "oldName": old_name, "movedFiles": []}

    nb_dir = Path(NOTEBOOKS_DIR)
    extensions = [".notebook.json", ".ipynb"]
    moved_files: list[str] = []

    source_paths = []
    for ext in extensions:
        src = nb_dir / f"{old_name}{ext}"
        dst = nb_dir / f"{new_name}{ext}"
        if src.exists():
            if dst.exists():
                raise HTTPException(status_code=409, detail=f"Target notebook already exists: {new_name}{ext}")
            source_paths.append((src, dst))

    if not source_paths:
        raise HTTPException(status_code=404, detail=f"Notebook '{old_name}' not found")

    for src, dst in source_paths:
        src.rename(dst)
        moved_files.append(dst.name)

    return {
        "status": "renamed",
        "oldName": old_name,
        "name": new_name,
        "movedFiles": moved_files,
    }


@router.get("/notebook/files/{name}")
async def load_notebook(name: str):
    """Load a saved notebook."""
    safe_name = _require_valid_notebook_name(name)

    filepath = Path(NOTEBOOKS_DIR) / f"{safe_name}.notebook.json"
    if filepath.exists():
        return json.loads(filepath.read_text(encoding="utf-8"))

    filepath_ipynb = Path(NOTEBOOKS_DIR) / f"{safe_name}.ipynb"
    if filepath_ipynb.exists():
        data = json.loads(filepath_ipynb.read_text(encoding="utf-8"))
        cells = []
        for cell in data.get("cells", []):
            cells.append({
                "id": str(uuid.uuid4()),
                "cell_type": cell.get("cell_type", "code"),
                "source": "".join(cell.get("source", [])),
                "language": "python",
                "outputs": cell.get("outputs", []),
                "execution_count": cell.get("execution_count"),
            })
        return {"name": safe_name, "cells": cells}

    raise HTTPException(status_code=404, detail=f"Notebook '{safe_name}' not found")


@router.delete("/notebook/files/{name}")
async def delete_notebook(name: str):
    """Delete notebook file variants (.notebook.json and .ipynb)."""
    safe_name = _require_valid_notebook_name(name)

    deleted_files: list[str] = []
    for ext in [".notebook.json", ".ipynb"]:
        filepath = Path(NOTEBOOKS_DIR) / f"{safe_name}{ext}"
        if filepath.exists():
            filepath.unlink()
            deleted_files.append(filepath.name)

    if not deleted_files:
        raise HTTPException(status_code=404, detail=f"Notebook '{safe_name}' not found")

    return {"status": "deleted", "name": safe_name, "deletedFiles": deleted_files}


# ─── Routes: Health ───

@router.get("/notebook/health")
async def notebook_health():
    """Check notebook execution engine status."""
    cleaned = await _cleanup_stale_sessions()
    gw_available = await _kernel_gw_available()
    return {
        "gateway_available": gw_available,
        "gateway_enabled": NOTEBOOK_USE_GATEWAY,
        "gateway_url": KERNEL_GW_URL,
        "mode": "gateway" if gw_available else "local",
        "active_sessions": len(_sessions),
        "cleaned_sessions": cleaned,
        "session_ttl_seconds": NOTEBOOK_SESSION_TTL_SECONDS,
        "sql_default_engine": NOTEBOOK_SQL_ENGINE,
        "spark_master_url": SPARK_MASTER_URL,
        "spark_default_database": SPARK_DEFAULT_DATABASE,
        "notebooks_dir": str(NOTEBOOKS_DIR),
    }
