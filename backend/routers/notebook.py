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
import shlex
import subprocess
import sys
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
_INVISIBLE_PREFIX_CHARS = "\ufeff\u200b\u200c\u200d\u2060"

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
SPARK_MASTER_UI_PUBLIC_URL = (os.getenv("SPARK_MASTER_UI_PUBLIC_URL", "http://localhost:8082") or "").strip()
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
SPARK_DELTA_WAREHOUSE = (
    os.getenv("SPARK_DELTA_WAREHOUSE", os.getenv("DELTA_WAREHOUSE", "s3a://deltalake/warehouse/"))
    or "s3a://deltalake/warehouse/"
).strip()
SPARK_EXTRA_PACKAGES = os.getenv("SPARK_EXTRA_PACKAGES", "")
SPARK_JARS_REPOSITORIES = os.getenv("SPARK_JARS_REPOSITORIES", "")
SPARK_REMOTE_URL = (os.getenv("SPARK_REMOTE_URL", "") or "").strip()
SPARK_ICEBERG_ENABLED = (os.getenv("SPARK_ICEBERG_ENABLED", "true") or "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SPARK_ICEBERG_CATALOG = (os.getenv("SPARK_ICEBERG_CATALOG", "lakehouse") or "lakehouse").strip()
SPARK_ICEBERG_CATALOG_URI = (
    os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://host.docker.internal:5433/controldb")
    or "jdbc:postgresql://host.docker.internal:5433/controldb"
).strip()
SPARK_ICEBERG_CATALOG_USER = (os.getenv("ICEBERG_CATALOG_USER", "postgres") or "postgres").strip()
SPARK_ICEBERG_CATALOG_PASSWORD = (os.getenv("ICEBERG_CATALOG_PASSWORD", "Gs+163264128") or "Gs+163264128").strip()
SPARK_ICEBERG_WAREHOUSE = (os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg/warehouse/") or "s3a://iceberg/warehouse/").strip()
SPARK_ICEBERG_CATALOG_ALIASES = tuple(
    sorted(
        {
            alias.strip().lower()
            for alias in (os.getenv("SPARK_ICEBERG_CATALOG_ALIASES", "") or "").split(",")
            if alias and alias.strip()
        }
    )
)
NOTEBOOK_SQL_ENGINE = (os.getenv("NOTEBOOK_SQL_ENGINE", "spark") or "spark").strip().lower()
if NOTEBOOK_SQL_ENGINE not in {"spark", "trino"}:
    NOTEBOOK_SQL_ENGINE = "spark"
NOTEBOOK_USE_GATEWAY = (os.getenv("NOTEBOOK_USE_GATEWAY", "false") or "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
NOTEBOOK_CLUSTER_DEFAULT = (os.getenv("NOTEBOOK_CLUSTER_DEFAULT", "small") or "small").strip().lower()
NOTEBOOK_CLUSTER_PROFILES_JSON = (os.getenv("NOTEBOOK_CLUSTER_PROFILES_JSON", "") or "").strip()
try:
    NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS = max(120, int(os.getenv("NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS", "900")))
except ValueError:
    NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS = 900

try:
    NOTEBOOK_SESSION_TTL_SECONDS = max(300, int(os.getenv("NOTEBOOK_SESSION_TTL_SECONDS", "7200")))
except ValueError:
    NOTEBOOK_SESSION_TTL_SECONDS = 7200

try:
    NOTEBOOK_TABLE_ROW_LIMIT = max(50, int(os.getenv("NOTEBOOK_TABLE_ROW_LIMIT", "500")))
except ValueError:
    NOTEBOOK_TABLE_ROW_LIMIT = 500
try:
    NOTEBOOK_SHELL_TIMEOUT_SECONDS = max(5, int(os.getenv("NOTEBOOK_SHELL_TIMEOUT_SECONDS", "600")))
except ValueError:
    NOTEBOOK_SHELL_TIMEOUT_SECONDS = 600

# Ensure notebooks directory exists
Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)


# ─── Models ───

class ExecuteRequest(BaseModel):
    code: str
    kernel_id: Optional[str] = None


class StartKernelRequest(BaseModel):
    cluster_id: Optional[str] = None


class AttachClusterRequest(BaseModel):
    cluster_id: str
    restart: bool = True


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
_spark_session_lock = threading.Lock()
_spark_query_lock = threading.Lock()
_spark_sessions: dict[str, Any] = {}
_cluster_runtime_state: dict[str, dict[str, Any]] = {}
_capacity_cache: dict[str, dict[str, Any]] = {}
_catalog_cache_lock = threading.Lock()
_spark_catalog_cache: dict[str, dict[str, Any]] = {}


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


_CLUSTER_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


def _env_int(name: str, default: int, min_value: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, value)


def _normalize_cluster_profile(raw: dict[str, Any], fallback_id: str) -> dict[str, Any]:
    cluster_id = str(raw.get("id") or fallback_id).strip().lower()
    if not cluster_id:
        cluster_id = fallback_id
    if not _CLUSTER_ID_PATTERN.match(cluster_id):
        raise ValueError(f"invalid cluster id: {cluster_id}")

    label = str(raw.get("label") or cluster_id).strip() or cluster_id
    description = str(raw.get("description") or "").strip()
    remote_url = str(raw.get("spark_remote_url") or raw.get("remote_url") or "").strip() or None
    master_url = str(raw.get("spark_master_url") or raw.get("master_url") or "").strip() or None

    spark_conf_raw = raw.get("spark_conf") if isinstance(raw.get("spark_conf"), dict) else {}
    spark_conf: dict[str, str] = {}
    for key, value in spark_conf_raw.items():
        k = str(key).strip()
        v = str(value).strip()
        if k and v:
            spark_conf[k] = v

    limits_raw = raw.get("limits") if isinstance(raw.get("limits"), dict) else {}
    max_rows_raw = limits_raw.get("max_rows", limits_raw.get("maxRows", NOTEBOOK_TABLE_ROW_LIMIT))
    max_rows = NOTEBOOK_TABLE_ROW_LIMIT
    try:
        max_rows = max(10, int(max_rows_raw))
    except Exception:
        pass

    idle_timeout_raw = limits_raw.get("idle_timeout_seconds", limits_raw.get("idleTimeoutSeconds", NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS))
    idle_timeout_seconds = NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS
    try:
        idle_timeout_seconds = max(60, int(idle_timeout_raw))
    except Exception:
        pass

    return {
        "id": cluster_id,
        "label": label,
        "description": description,
        "spark_remote_url": remote_url,
        "spark_master_url": master_url,
        "spark_conf": spark_conf,
        "limits": {
            "max_rows": max_rows,
            "idle_timeout_seconds": idle_timeout_seconds,
        },
    }


def _default_cluster_profiles() -> dict[str, dict[str, Any]]:
    small_cores = _env_int("NOTEBOOK_CLUSTER_SMALL_EXECUTOR_CORES", 2)
    medium_cores = _env_int("NOTEBOOK_CLUSTER_MEDIUM_EXECUTOR_CORES", 4)
    large_cores = _env_int("NOTEBOOK_CLUSTER_LARGE_EXECUTOR_CORES", 8)
    small_shuffle = _env_int("NOTEBOOK_CLUSTER_SMALL_SHUFFLE_PARTITIONS", max(4, small_cores * 4))
    medium_shuffle = _env_int("NOTEBOOK_CLUSTER_MEDIUM_SHUFFLE_PARTITIONS", max(4, medium_cores * 4))
    large_shuffle = _env_int("NOTEBOOK_CLUSTER_LARGE_SHUFFLE_PARTITIONS", max(4, large_cores * 4))

    small = _normalize_cluster_profile(
        {
            "id": "small",
            "label": "Small",
            "description": "Light development profile",
            "spark_remote_url": SPARK_REMOTE_URL or None,
            "spark_master_url": SPARK_MASTER_URL,
            "spark_conf": {
                "spark.executor.instances": str(_env_int("NOTEBOOK_CLUSTER_SMALL_EXECUTOR_INSTANCES", 1)),
                "spark.executor.cores": str(small_cores),
                "spark.executor.memory": os.getenv("NOTEBOOK_CLUSTER_SMALL_EXECUTOR_MEMORY", "2g"),
                "spark.sql.shuffle.partitions": str(small_shuffle),
            },
            "limits": {
                "max_rows": _env_int("NOTEBOOK_CLUSTER_SMALL_MAX_ROWS", NOTEBOOK_TABLE_ROW_LIMIT, min_value=10),
                "idle_timeout_seconds": _env_int(
                    "NOTEBOOK_CLUSTER_SMALL_IDLE_TIMEOUT_SECONDS",
                    NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS,
                    min_value=60,
                ),
            },
        },
        "small",
    )
    medium = _normalize_cluster_profile(
        {
            "id": "medium",
            "label": "Medium",
            "description": "Balanced profile for transformations",
            "spark_remote_url": SPARK_REMOTE_URL or None,
            "spark_master_url": SPARK_MASTER_URL,
            "spark_conf": {
                "spark.executor.instances": str(_env_int("NOTEBOOK_CLUSTER_MEDIUM_EXECUTOR_INSTANCES", 1)),
                "spark.executor.cores": str(medium_cores),
                "spark.executor.memory": os.getenv("NOTEBOOK_CLUSTER_MEDIUM_EXECUTOR_MEMORY", "4g"),
                "spark.sql.shuffle.partitions": str(medium_shuffle),
            },
            "limits": {
                "max_rows": _env_int("NOTEBOOK_CLUSTER_MEDIUM_MAX_ROWS", NOTEBOOK_TABLE_ROW_LIMIT, min_value=10),
                "idle_timeout_seconds": _env_int(
                    "NOTEBOOK_CLUSTER_MEDIUM_IDLE_TIMEOUT_SECONDS",
                    NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS,
                    min_value=60,
                ),
            },
        },
        "medium",
    )
    large = _normalize_cluster_profile(
        {
            "id": "large",
            "label": "Large",
            "description": "High-throughput profile for heavy transforms",
            "spark_remote_url": SPARK_REMOTE_URL or None,
            "spark_master_url": SPARK_MASTER_URL,
            "spark_conf": {
                "spark.executor.instances": str(_env_int("NOTEBOOK_CLUSTER_LARGE_EXECUTOR_INSTANCES", 1)),
                "spark.executor.cores": str(large_cores),
                "spark.executor.memory": os.getenv("NOTEBOOK_CLUSTER_LARGE_EXECUTOR_MEMORY", "8g"),
                "spark.sql.shuffle.partitions": str(large_shuffle),
            },
            "limits": {
                "max_rows": _env_int("NOTEBOOK_CLUSTER_LARGE_MAX_ROWS", NOTEBOOK_TABLE_ROW_LIMIT, min_value=10),
                "idle_timeout_seconds": _env_int(
                    "NOTEBOOK_CLUSTER_LARGE_IDLE_TIMEOUT_SECONDS",
                    NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS,
                    min_value=60,
                ),
            },
        },
        "large",
    )
    return {item["id"]: item for item in [small, medium, large]}


def _load_cluster_profiles() -> dict[str, dict[str, Any]]:
    if not NOTEBOOK_CLUSTER_PROFILES_JSON:
        return _default_cluster_profiles()

    try:
        raw = json.loads(NOTEBOOK_CLUSTER_PROFILES_JSON)
    except json.JSONDecodeError as exc:
        logger.warning("Invalid NOTEBOOK_CLUSTER_PROFILES_JSON. Falling back to defaults: %s", exc)
        return _default_cluster_profiles()

    normalized: dict[str, dict[str, Any]] = {}
    try:
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if not isinstance(item, dict):
                    continue
                profile = _normalize_cluster_profile(item, f"cluster_{idx + 1}")
                normalized[profile["id"]] = profile
        elif isinstance(raw, dict):
            for key, item in raw.items():
                if isinstance(item, dict):
                    merged = {"id": str(key), **item}
                else:
                    continue
                profile = _normalize_cluster_profile(merged, str(key))
                normalized[profile["id"]] = profile
    except Exception as exc:
        logger.warning("Invalid cluster profile definition. Falling back to defaults: %s", exc)
        return _default_cluster_profiles()

    if not normalized:
        return _default_cluster_profiles()
    return normalized


_CLUSTER_PROFILES = _load_cluster_profiles()
if NOTEBOOK_CLUSTER_DEFAULT in _CLUSTER_PROFILES:
    _DEFAULT_CLUSTER_ID = NOTEBOOK_CLUSTER_DEFAULT
else:
    _DEFAULT_CLUSTER_ID = next(iter(_CLUSTER_PROFILES.keys()))


def _resolve_cluster_id(cluster_id: Optional[str]) -> str:
    candidate = (cluster_id or "").strip().lower()
    if not candidate:
        return _DEFAULT_CLUSTER_ID
    if candidate not in _CLUSTER_PROFILES:
        raise HTTPException(status_code=400, detail=f"Unknown cluster profile: {candidate}")
    return candidate


def _cluster_profile(cluster_id: Optional[str]) -> dict[str, Any]:
    resolved = _resolve_cluster_id(cluster_id)
    return _CLUSTER_PROFILES[resolved]


def _cluster_runtime(cluster_id: str) -> dict[str, Any]:
    return _cluster_runtime_state.setdefault(
        cluster_id,
        {
            "status": "idle",
            "last_used": None,
            "started_at": None,
            "last_stopped": None,
            "active_sessions": 0,
            "auto_stops": 0,
            "last_error": None,
            "spark_ui_url": None,
            "application_id": None,
            "effective_resources": None,
            "available_resources": None,
        },
    )


def _safe_int(value: Any, default: int = 0, *, min_value: Optional[int] = None) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return default
    if min_value is not None:
        return max(min_value, parsed)
    return parsed


def _memory_to_mb(value: Any, default_mb: int = 0) -> int:
    if value is None:
        return default_mb
    text = str(value).strip().lower()
    if not text:
        return default_mb
    try:
        if text.endswith("g"):
            return int(float(text[:-1]) * 1024)
        if text.endswith("m"):
            return int(float(text[:-1]))
        if text.endswith("k"):
            return max(1, int(float(text[:-1]) / 1024))
        return int(float(text))
    except Exception:
        return default_mb


def _format_memory_mb(mb: int) -> str:
    if mb <= 0:
        return "0m"
    if mb % 1024 == 0:
        return f"{mb // 1024}g"
    return f"{mb}m"


def _capacity_summary_from_workers(workers: list[dict[str, Any]]) -> dict[str, Any]:
    cores_total = 0
    memory_total_mb = 0
    for worker in workers:
        cores_total += int(worker.get("cores", 0) or 0)
        memory_total_mb += int(worker.get("memory", 0) or 0)
    return {
        "workers": len(workers),
        "total_cores": cores_total,
        "total_memory_mb": memory_total_mb,
        "total_memory": _format_memory_mb(memory_total_mb),
    }


def _fetch_master_capacity_sync(master_ui_internal: Optional[str]) -> Optional[dict[str, Any]]:
    if not master_ui_internal:
        return None
    cache_key = master_ui_internal.rstrip("/")
    now_ts = _now_utc().timestamp()
    cached = _capacity_cache.get(cache_key)
    if cached and (now_ts - float(cached.get("ts", 0))) < 8:
        data = cached.get("data")
        if isinstance(data, dict):
            return data

    try:
        with httpx.Client(timeout=1.5) as client:
            response = client.get(f"{cache_key}/json/")
            if response.status_code >= 400:
                return None
            payload = response.json() if response.text else {}
    except Exception:
        return None

    workers = payload.get("workers") if isinstance(payload.get("workers"), list) else []
    normalized_workers = [w for w in workers if isinstance(w, dict)]
    summary = _capacity_summary_from_workers(normalized_workers)
    _capacity_cache[cache_key] = {"ts": now_ts, "data": summary}
    return summary


async def _fetch_master_capacity_async(master_ui_internal: Optional[str]) -> Optional[dict[str, Any]]:
    if not master_ui_internal:
        return None
    cache_key = master_ui_internal.rstrip("/")
    now_ts = _now_utc().timestamp()
    cached = _capacity_cache.get(cache_key)
    if cached and (now_ts - float(cached.get("ts", 0))) < 8:
        data = cached.get("data")
        if isinstance(data, dict):
            return data

    try:
        payload = await _fetch_json(f"{cache_key}/json/", timeout_seconds=1.5)
    except Exception:
        return None

    workers = payload.get("workers") if isinstance(payload.get("workers"), list) else []
    normalized_workers = [w for w in workers if isinstance(w, dict)]
    summary = _capacity_summary_from_workers(normalized_workers)
    _capacity_cache[cache_key] = {"ts": now_ts, "data": summary}
    return summary


def _profile_resource_summary(profile: dict[str, Any]) -> dict[str, Any]:
    conf = dict(profile.get("spark_conf") or {})
    instances = _safe_int(conf.get("spark.executor.instances"), 1, min_value=1)
    cores = _safe_int(conf.get("spark.executor.cores"), 1, min_value=1)
    memory_text = str(conf.get("spark.executor.memory") or "1g")
    memory_mb = _memory_to_mb(memory_text, 1024)
    shuffle_partitions = _safe_int(conf.get("spark.sql.shuffle.partitions"), _safe_int(SPARK_SQL_SHUFFLE_PARTITIONS, 8), min_value=1)
    total_cores = instances * cores
    total_memory_mb = instances * memory_mb
    return {
        "executor_instances": instances,
        "executor_cores": cores,
        "executor_memory": _format_memory_mb(memory_mb),
        "executor_memory_mb": memory_mb,
        "total_cores": total_cores,
        "total_memory_mb": total_memory_mb,
        "shuffle_partitions": shuffle_partitions,
    }


def _effective_cluster_resources(cluster_id: str) -> dict[str, Any]:
    profile = _cluster_profile(cluster_id)
    summary = _profile_resource_summary(profile)
    spark_session = _spark_sessions.get(cluster_id)
    if spark_session is None:
        return summary

    def get_conf(key: str, fallback: str) -> str:
        try:
            value = spark_session.conf.get(key)
            return str(value).strip() or fallback
        except Exception:
            return fallback

    instances = _safe_int(get_conf("spark.executor.instances", str(summary["executor_instances"])), summary["executor_instances"], min_value=1)
    cores = _safe_int(get_conf("spark.executor.cores", str(summary["executor_cores"])), summary["executor_cores"], min_value=1)
    memory_text = get_conf("spark.executor.memory", str(summary["executor_memory"]))
    memory_mb = _memory_to_mb(memory_text, int(summary["executor_memory_mb"]))
    shuffle_partitions = _safe_int(
        get_conf("spark.sql.shuffle.partitions", str(summary["shuffle_partitions"])),
        int(summary["shuffle_partitions"]),
        min_value=1,
    )
    total_cores = instances * cores
    total_memory_mb = instances * memory_mb
    return {
        "executor_instances": instances,
        "executor_cores": cores,
        "executor_memory": _format_memory_mb(memory_mb),
        "executor_memory_mb": memory_mb,
        "total_cores": total_cores,
        "total_memory_mb": total_memory_mb,
        "shuffle_partitions": shuffle_partitions,
    }


def _cap_profile_resources_to_available(
    profile_resources: dict[str, Any],
    available_resources: Optional[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(available_resources, dict):
        return dict(profile_resources)

    total_cores = _safe_int(available_resources.get("total_cores"), 0)
    total_memory_mb = _safe_int(available_resources.get("total_memory_mb"), 0)
    instances = _safe_int(profile_resources.get("executor_instances"), 1, min_value=1)
    cores = _safe_int(profile_resources.get("executor_cores"), 1, min_value=1)
    memory_mb = _safe_int(profile_resources.get("executor_memory_mb"), 1024, min_value=1)
    shuffle_partitions = _safe_int(profile_resources.get("shuffle_partitions"), _safe_int(SPARK_SQL_SHUFFLE_PARTITIONS, 8), min_value=1)

    if total_cores > 0:
        cores = min(cores, total_cores)
        instances = min(instances, max(1, total_cores // max(1, cores)))
    if total_memory_mb > 0:
        memory_mb = min(memory_mb, total_memory_mb)
        instances = min(instances, max(1, total_memory_mb // max(1, memory_mb)))

    return {
        "executor_instances": max(1, instances),
        "executor_cores": max(1, cores),
        "executor_memory": _format_memory_mb(max(1, memory_mb)),
        "executor_memory_mb": max(1, memory_mb),
        "total_cores": max(1, instances) * max(1, cores),
        "total_memory_mb": max(1, instances) * max(1, memory_mb),
        "shuffle_partitions": shuffle_partitions,
    }


def _shuffle_tuning_from_resources(resources: Optional[dict[str, Any]]) -> dict[str, Any]:
    source = resources or {}
    total_cores = _safe_int(source.get("total_cores"), 0, min_value=0)
    current_shuffle = _safe_int(source.get("shuffle_partitions"), _safe_int(SPARK_SQL_SHUFFLE_PARTITIONS, 8), min_value=1)
    if total_cores <= 0:
        return {
            "current": current_shuffle,
            "recommended": current_shuffle,
            "recommended_min": max(1, current_shuffle // 2),
            "recommended_max": current_shuffle * 2,
            "status": "unknown",
            "message": "Cluster core capacity is unknown; shuffle recommendation unavailable.",
        }

    recommended_min = max(2, total_cores * 2)
    recommended = max(4, total_cores * 4)
    recommended_max = max(recommended_min, total_cores * 8)
    if current_shuffle < recommended_min:
        status = "low"
        message = f"Shuffle partitions is low for {total_cores} cores; increase toward ~{recommended}."
    elif current_shuffle > recommended_max:
        status = "high"
        message = f"Shuffle partitions is high for {total_cores} cores; reduce toward ~{recommended}."
    else:
        status = "ok"
        message = f"Shuffle partitions is in a healthy range for {total_cores} cores."

    return {
        "current": current_shuffle,
        "recommended": recommended,
        "recommended_min": recommended_min,
        "recommended_max": recommended_max,
        "status": status,
        "message": message,
    }


def _count_sessions_for_cluster(cluster_id: str) -> int:
    return sum(1 for info in _sessions.values() if info.get("cluster_id") == cluster_id)


def _touch_cluster(cluster_id: str) -> None:
    runtime = _cluster_runtime(cluster_id)
    runtime["last_used"] = _now_iso()
    runtime["active_sessions"] = _count_sessions_for_cluster(cluster_id)


def _session_cluster_id(session_id: str) -> str:
    session = _sessions.get(session_id) or {}
    cluster_id = str(session.get("cluster_id") or "").strip().lower()
    if cluster_id in _CLUSTER_PROFILES:
        return cluster_id
    return _DEFAULT_CLUSTER_ID


def _cluster_master_url(cluster_id: str) -> str:
    profile = _cluster_profile(cluster_id)
    return str(profile.get("spark_master_url") or SPARK_MASTER_URL).strip()


def _cluster_remote_url(cluster_id: str) -> str:
    profile = _cluster_profile(cluster_id)
    return str(profile.get("spark_remote_url") or "").strip()


def _spark_master_ui_url(master_url: str, *, public: bool = False) -> Optional[str]:
    if public and SPARK_MASTER_UI_PUBLIC_URL:
        return SPARK_MASTER_UI_PUBLIC_URL.rstrip("/")
    url = str(master_url or "").strip()
    if not url or url.lower().startswith("local"):
        return None
    if url.startswith("spark://"):
        host_port = url.replace("spark://", "", 1)
        host = host_port.split(":")[0].strip()
        if not host:
            return None
        return f"http://{host}:8080"
    if url.startswith("http://") or url.startswith("https://"):
        return url.rstrip("/")
    return None


def _touch_session(session_id: str) -> None:
    session = _sessions.get(session_id)
    if session:
        session["last_used"] = _now_iso()
        cluster_id = str(session.get("cluster_id") or "").strip().lower()
        if cluster_id in _CLUSTER_PROFILES:
            _touch_cluster(cluster_id)


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

    await asyncio.to_thread(_cleanup_idle_spark_sessions)
    return len(stale_ids)


def _cleanup_idle_spark_sessions() -> int:
    stopped = 0
    now = _now_utc()
    with _spark_session_lock:
        for cluster_id in _CLUSTER_PROFILES:
            runtime = _cluster_runtime(cluster_id)
            active_sessions = _count_sessions_for_cluster(cluster_id)
            runtime["active_sessions"] = active_sessions
            if active_sessions > 0:
                if cluster_id in _spark_sessions:
                    runtime["status"] = "running"
                continue

            spark_session = _spark_sessions.get(cluster_id)
            if spark_session is None:
                if runtime.get("status") != "auto-stopped":
                    runtime["status"] = "idle"
                continue

            timeout_seconds = int(
                _cluster_profile(cluster_id).get("limits", {}).get("idle_timeout_seconds", NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS)
            )
            last_used = _parse_iso(runtime.get("last_used")) or _parse_iso(runtime.get("started_at"))
            if last_used is None:
                last_used = now
            idle_seconds = max(0.0, (now - last_used).total_seconds())
            if idle_seconds < timeout_seconds:
                runtime["status"] = "idle"
                continue

            try:
                spark_session.stop()
            except Exception as exc:
                runtime["last_error"] = str(exc)
                logger.warning("Failed to stop Spark session for cluster '%s': %s", cluster_id, exc)
            _spark_sessions.pop(cluster_id, None)
            runtime["status"] = "auto-stopped"
            runtime["last_stopped"] = _now_iso()
            runtime["auto_stops"] = int(runtime.get("auto_stops") or 0) + 1
            runtime["spark_ui_url"] = None
            runtime["application_id"] = None
            runtime["effective_resources"] = None
            stopped += 1

    return stopped


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


def _strip_leading_invisible(text: str) -> str:
    return str(text or "").lstrip(_INVISIBLE_PREFIX_CHARS)


def _strip_sql_magic_prefix(raw_sql: str) -> str:
    text = _strip_leading_invisible(raw_sql or "")
    lines = text.splitlines()
    if not lines:
        return text

    first_idx: Optional[int] = None
    for idx, line in enumerate(lines):
        if _strip_leading_invisible(line).strip():
            first_idx = idx
            break

    if first_idx is None:
        return text

    first = _strip_leading_invisible(lines[first_idx]).strip()
    magic_match = re.match(r"^(%%?(?:sql|spark))(?:\s+(.*))?$", first, re.IGNORECASE)
    if not magic_match:
        return text

    inline = (magic_match.group(2) or "").strip()
    remainder = "\n".join(lines[first_idx + 1 :]).strip()
    if inline and remainder:
        return f"{inline}\n{remainder}"
    if inline:
        return inline
    return remainder


def _normalize_spark_iceberg_catalog_alias(sql_text: str) -> str:
    text = str(sql_text or "")
    target_catalog = str(SPARK_ICEBERG_CATALOG or "").strip()
    if not text or not target_catalog:
        return text

    target_lower = target_catalog.lower()
    aliases = [alias for alias in SPARK_ICEBERG_CATALOG_ALIASES if alias != target_lower]
    if not aliases:
        return text

    normalized = text
    for alias in aliases:
        normalized = re.sub(
            rf"(?i)(?<![A-Za-z0-9_`]){re.escape(alias)}\.",
            f"{target_catalog}.",
            normalized,
        )
        normalized = re.sub(
            rf"(?i)`{re.escape(alias)}`\.",
            f"`{target_catalog}`.",
            normalized,
        )
    return normalized


def _prepare_spark_sql_text(statement: str) -> str:
    return _sanitize_sql(_normalize_spark_iceberg_catalog_alias(_strip_sql_magic_prefix(statement)))


def _discover_spark_catalogs(spark: Any, cluster_id: Optional[str] = None) -> list[str]:
    resolved_cluster_id = _resolve_cluster_id(cluster_id)
    cache_key = resolved_cluster_id
    now_ts = _now_utc().timestamp()
    with _catalog_cache_lock:
        cached = _spark_catalog_cache.get(cache_key)
        if cached and (now_ts - float(cached.get("ts", 0))) < float(cached.get("ttl", 20)):
            data = cached.get("catalogs")
            if isinstance(data, list):
                return [str(x) for x in data if str(x).strip()]

    catalogs: list[str] = []
    discovery_ok = True
    try:
        rows = spark.sql("SHOW CATALOGS").collect()
        for row in rows:
            name = ""
            if hasattr(row, "asDict"):
                row_dict = row.asDict()
                name = str(row_dict.get("catalog") or row_dict.get("namespace") or row[0] or "").strip()
            else:
                name = str(row[0] if row else "").strip()
            if name and name not in catalogs:
                catalogs.append(name)
    except Exception:
        discovery_ok = False
        catalogs = []

    if SPARK_ICEBERG_ENABLED and SPARK_ICEBERG_CATALOG:
        configured = str(SPARK_ICEBERG_CATALOG).strip()
        if configured and configured not in catalogs:
            catalogs.insert(0, configured)

    if "spark_catalog" not in [c.lower() for c in catalogs]:
        catalogs.append("spark_catalog")

    with _catalog_cache_lock:
        _spark_catalog_cache[cache_key] = {
            "ts": now_ts,
            "ttl": 20 if discovery_ok else 2,
            "catalogs": list(catalogs),
        }
    return catalogs


def _extract_invalid_namespace_catalog(exc: Exception) -> Optional[str]:
    message = str(exc or "")
    if "REQUIRES_SINGLE_PART_NAMESPACE" not in message:
        return None

    match = re.search(r"got\s+`([^`]+)`\.`([^`]+)`", message, flags=re.IGNORECASE)
    if match:
        catalog = str(match.group(1) or "").strip()
        return catalog or None

    match = re.search(r"got\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", message, flags=re.IGNORECASE)
    if match:
        catalog = str(match.group(1) or "").strip()
        return catalog or None

    return None


def _extract_three_part_prefixes(sql_text: str) -> list[str]:
    text = str(sql_text or "")
    if not text:
        return []
    pattern = re.compile(
        r"(?i)(?<![A-Za-z0-9_`])`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?"
    )
    seen: list[str] = []
    for match in pattern.finditer(text):
        prefix = str(match.group(1) or "").strip()
        if prefix and prefix not in seen:
            seen.append(prefix)
    return seen


def _rewrite_catalog_prefix(sql_text: str, old_catalog: str, new_catalog: str) -> str:
    old = str(old_catalog or "").strip()
    new = str(new_catalog or "").strip()
    text = str(sql_text or "")
    if not old or not new or old.lower() == new.lower():
        return text

    rewritten = re.sub(
        rf"(?i)(?<![A-Za-z0-9_`]){re.escape(old)}\.",
        f"{new}.",
        text,
    )
    rewritten = re.sub(
        rf"(?i)`{re.escape(old)}`\.",
        f"`{new}`.",
        rewritten,
    )
    return rewritten


def _catalog_rewrite_targets(
    sql_text: str,
    spark: Any,
    cluster_id: Optional[str],
    exc: Exception,
) -> list[tuple[str, str]]:
    error_message = str(exc or "")
    discovered_catalogs = _discover_spark_catalogs(spark, cluster_id)
    discovered_lower = {str(c).strip().lower() for c in discovered_catalogs}

    preferred_targets: list[str] = []
    if SPARK_ICEBERG_ENABLED and SPARK_ICEBERG_CATALOG:
        configured = str(SPARK_ICEBERG_CATALOG).strip()
        if configured:
            preferred_targets.append(configured)
    preferred_targets.extend([str(c) for c in discovered_catalogs if str(c).strip().lower() != "spark_catalog"])
    preferred_targets.extend([str(c) for c in discovered_catalogs if str(c).strip().lower() == "spark_catalog"])
    # preserve order while removing duplicates
    normalized_targets: list[str] = []
    seen_targets: set[str] = set()
    for target in preferred_targets:
        key = str(target).strip().lower()
        if not key or key in seen_targets:
            continue
        seen_targets.add(key)
        normalized_targets.append(str(target).strip())
    preferred_targets = normalized_targets

    source_catalogs: list[str] = []
    unresolved_catalog = _extract_invalid_namespace_catalog(exc)
    if unresolved_catalog:
        source_catalogs.append(unresolved_catalog)

    if "TABLE_OR_VIEW_NOT_FOUND" in error_message:
        for prefix in _extract_three_part_prefixes(sql_text):
            if prefix.lower() not in discovered_lower and prefix not in source_catalogs:
                source_catalogs.append(prefix)

    if not source_catalogs:
        return []

    candidates: list[tuple[str, str]] = []
    for source in source_catalogs:
        for target in preferred_targets:
            target_name = str(target).strip()
            if not target_name or target_name.lower() == source.lower():
                continue
            candidates.append((source, target_name))
    return candidates


class _SparkSessionProxy:
    """Wrap SparkSession to normalize/retry SQL while keeping native API access."""

    def __init__(self, cluster_id: str):
        self._cluster_id = _resolve_cluster_id(cluster_id)

    def sql(self, statement: str, *args, **kwargs):
        sql_text = _prepare_spark_sql_text(statement)
        if not sql_text:
            raise RuntimeError("Spark SQL statement is empty")

        sql_candidates: list[str] = [sql_text]
        session_retry_done = False
        rewrites_generated = False
        last_exception: Optional[Exception] = None

        while sql_candidates:
            current_sql = sql_candidates.pop(0)
            try:
                spark = _get_spark_session(self._cluster_id)
                return spark.sql(current_sql, *args, **kwargs)
            except Exception as exc:
                last_exception = exc
                if not session_retry_done and _is_spark_session_closed_error(exc):
                    logger.warning(
                        "Spark session handle closed for cluster '%s' in SparkSessionProxy.sql; recreating session.",
                        self._cluster_id,
                    )
                    _invalidate_spark_session(self._cluster_id, reason=str(exc))
                    session_retry_done = True
                    sql_candidates.insert(0, current_sql)
                    continue
                if not rewrites_generated:
                    try:
                        spark = _get_spark_session(self._cluster_id)
                        rewrite_targets = _catalog_rewrite_targets(current_sql, spark, self._cluster_id, exc)
                        if rewrite_targets:
                            sql_candidates.extend(
                                [_rewrite_catalog_prefix(current_sql, old, target) for old, target in rewrite_targets]
                            )
                            rewrites_generated = True
                            logger.info(
                                "Detected unresolved Spark catalog prefix in SparkSessionProxy.sql; retrying with discovered catalogs. cluster=%s options=%s",
                                self._cluster_id,
                                [target for _, target in rewrite_targets],
                            )
                            continue
                    except Exception:
                        pass
                continue

        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Spark SQL execution failed")

    def __getattr__(self, name: str):
        spark = _get_spark_session(self._cluster_id)
        return getattr(spark, name)


def _split_sql_statements(sql_text: str) -> list[str]:
    sql = _sanitize_sql(sql_text)
    if not sql:
        return []

    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    escape = False

    for ch in sql:
        if escape:
            current.append(ch)
            escape = False
            continue

        if ch == "\\" and (in_single or in_double):
            current.append(ch)
            escape = True
            continue

        if ch == "'" and not in_double and not in_backtick:
            in_single = not in_single
            current.append(ch)
            continue
        if ch == '"' and not in_single and not in_backtick:
            in_double = not in_double
            current.append(ch)
            continue
        if ch == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
            current.append(ch)
            continue

        if ch == ";" and not in_single and not in_double and not in_backtick:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue

        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def _parse_cell_magic(code: str) -> tuple[Optional[str], str]:
    normalized_code = _strip_leading_invisible(code or "")
    lines = normalized_code.splitlines()
    if not lines:
        return None, ""

    first_idx: Optional[int] = None
    for idx, line in enumerate(lines):
        if _strip_leading_invisible(line).strip():
            first_idx = idx
            break

    if first_idx is None:
        return None, ""

    first = _strip_leading_invisible(lines[first_idx]).strip()

    if first.startswith("%%"):
        token = first.split(None, 1)[0].lower()
        inline = first[len(token):].strip()
        remainder = "\n".join(lines[first_idx + 1 :]).strip()
        payload = inline if not remainder else (f"{inline}\n{remainder}" if inline else remainder)
        return token, payload

    if first.startswith("%"):
        token = first.split(None, 1)[0].lower()
        inline = first[len(token):].strip()
        remainder = "\n".join(lines[first_idx + 1 :]).strip()
        payload = inline if not remainder else (f"{inline}\n{remainder}" if inline else remainder)
        return token, payload

    if first.startswith("!"):
        inline = first[1:].strip()
        remainder = "\n".join(lines[first_idx + 1 :]).strip()
        payload = inline if not remainder else (f"{inline}\n{remainder}" if inline else remainder)
        return "!", payload

    return None, normalized_code


def _run_shell_command(command: str, execution_count: int) -> dict[str, Any]:
    cmd = str(command or "").strip()
    if not cmd:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "ValueError",
                "evalue": "Shell command is empty",
                "traceback": [],
            }],
        }

    try:
        proc = subprocess.run(
            cmd,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
            timeout=NOTEBOOK_SHELL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "TimeoutError",
                "evalue": f"Shell command timed out after {NOTEBOOK_SHELL_TIMEOUT_SECONDS}s",
                "traceback": [],
            }],
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

    outputs: list[dict[str, Any]] = []
    if proc.stdout:
        outputs.append({"output_type": "stream", "name": "stdout", "text": proc.stdout})
    if proc.stderr:
        outputs.append({"output_type": "stream", "name": "stderr", "text": proc.stderr})

    if not outputs:
        outputs.append({"output_type": "stream", "name": "stdout", "text": ""})

    return {
        "status": "ok" if proc.returncode == 0 else "error",
        "execution_count": execution_count,
        "outputs": outputs,
    }


def _run_pip_magic(arguments: str, execution_count: int) -> dict[str, Any]:
    args_raw = str(arguments or "").strip()
    if not args_raw:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "ValueError",
                "evalue": "Usage: %pip install <package>",
                "traceback": [],
            }],
        }

    try:
        parsed = shlex.split(args_raw)
    except ValueError as exc:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "ValueError",
                "evalue": f"Invalid pip arguments: {exc}",
                "traceback": [],
            }],
        }

    if parsed and parsed[0] == "pip":
        parsed = parsed[1:]
    if not parsed:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "ValueError",
                "evalue": "Usage: %pip install <package>",
                "traceback": [],
            }],
        }

    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pip", *parsed],
            check=False,
            capture_output=True,
            text=True,
            timeout=NOTEBOOK_SHELL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "execution_count": execution_count,
            "outputs": [{
                "output_type": "error",
                "ename": "TimeoutError",
                "evalue": f"pip command timed out after {NOTEBOOK_SHELL_TIMEOUT_SECONDS}s",
                "traceback": [],
            }],
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

    outputs: list[dict[str, Any]] = []
    if proc.stdout:
        outputs.append({"output_type": "stream", "name": "stdout", "text": proc.stdout})
    if proc.stderr:
        outputs.append({"output_type": "stream", "name": "stderr", "text": proc.stderr})

    return {
        "status": "ok" if proc.returncode == 0 else "error",
        "execution_count": execution_count,
        "outputs": outputs or [{"output_type": "stream", "name": "stdout", "text": ""}],
    }


def _render_markdown_output(markdown_text: str, execution_count: int) -> dict[str, Any]:
    return {
        "status": "ok",
        "execution_count": execution_count,
        "outputs": [{
            "output_type": "display_data",
            "data": {
                "text/markdown": markdown_text,
            },
            "execution_count": execution_count,
        }],
    }


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


def _spark_session_usable(session: Any, *, remote: bool = False) -> bool:
    if session is None:
        return False
    if remote:
        try:
            _ = session.version
            return True
        except Exception:
            return False
    try:
        spark_context = session.sparkContext
        jsc = getattr(spark_context, "_jsc", None)
        if jsc is None:
            return False
        return not bool(jsc.sc().isStopped())
    except Exception:
        return False


def _is_spark_session_closed_error(exc: Exception) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    if "invalid_handle.session_closed" in message or "session_closed" in message:
        return True
    if "session was closed" in message or "session is closed" in message:
        return True
    if "handle" in message and "is invalid" in message and "session" in message:
        return True
    return False


def _invalidate_spark_session(cluster_id: Optional[str] = None, *, reason: Optional[str] = None) -> None:
    resolved_cluster_id = _resolve_cluster_id(cluster_id)
    with _spark_session_lock:
        spark_session = _spark_sessions.pop(resolved_cluster_id, None)

    if spark_session is not None:
        try:
            spark_session.stop()
        except Exception:
            pass

    _reset_spark_singletons()
    runtime = _cluster_runtime(resolved_cluster_id)
    runtime["status"] = "idle"
    runtime["last_stopped"] = _now_iso()
    runtime["spark_ui_url"] = None
    runtime["application_id"] = None
    runtime["effective_resources"] = None
    runtime["active_sessions"] = _count_sessions_for_cluster(resolved_cluster_id)
    if reason:
        runtime["last_error"] = str(reason)


def _spark_runtime_mode(cluster_id: Optional[str] = None) -> str:
    remote_url = _cluster_remote_url(_resolve_cluster_id(cluster_id))
    master_url = _cluster_master_url(_resolve_cluster_id(cluster_id))
    if remote_url:
        return "spark_connect"
    if master_url.strip().lower().startswith("local"):
        return "local"
    return "standalone"


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


def _get_spark_session(cluster_id: Optional[str] = None):
    resolved_cluster_id = _resolve_cluster_id(cluster_id)
    profile = _cluster_profile(resolved_cluster_id)
    remote_url = _cluster_remote_url(resolved_cluster_id)
    master_url = _cluster_master_url(resolved_cluster_id)
    master_ui_internal = _spark_master_ui_url(master_url, public=False)
    use_remote = bool(remote_url)

    existing = _spark_sessions.get(resolved_cluster_id)
    if _spark_session_usable(existing, remote=use_remote):
        runtime = _cluster_runtime(resolved_cluster_id)
        runtime["effective_resources"] = _effective_cluster_resources(resolved_cluster_id)
        runtime["available_resources"] = _fetch_master_capacity_sync(master_ui_internal)
        _touch_cluster(resolved_cluster_id)
        return existing

    with _spark_session_lock:
        existing = _spark_sessions.get(resolved_cluster_id)
        if _spark_session_usable(existing, remote=use_remote):
            runtime = _cluster_runtime(resolved_cluster_id)
            runtime["effective_resources"] = _effective_cluster_resources(resolved_cluster_id)
            runtime["available_resources"] = _fetch_master_capacity_sync(master_ui_internal)
            _touch_cluster(resolved_cluster_id)
            return existing

        _reset_spark_singletons()
        spark_sql = importlib.import_module("pyspark.sql")
        SparkSession = spark_sql.SparkSession
        app_name = f"{SPARK_APP_NAME}-{resolved_cluster_id}"
        builder = SparkSession.builder.appName(app_name)
        if use_remote:
            builder = builder.remote(remote_url)
        else:
            builder = builder.master(master_url)

        builder = (
            builder
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.warehouse.dir", SPARK_DELTA_WAREHOUSE)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", str(SPARK_S3_PATH_STYLE_ACCESS).lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", SPARK_S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SPARK_S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(SPARK_S3_ENDPOINT.startswith("https://")).lower())
        )
        if SPARK_ICEBERG_ENABLED and SPARK_ICEBERG_CATALOG:
            iceberg_catalog = SPARK_ICEBERG_CATALOG
            builder = (
                builder
                .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
                .config(f"spark.sql.catalog.{iceberg_catalog}.type", "jdbc")
                .config(f"spark.sql.catalog.{iceberg_catalog}.uri", SPARK_ICEBERG_CATALOG_URI)
                .config(f"spark.sql.catalog.{iceberg_catalog}.jdbc.user", SPARK_ICEBERG_CATALOG_USER)
                .config(f"spark.sql.catalog.{iceberg_catalog}.jdbc.password", SPARK_ICEBERG_CATALOG_PASSWORD)
                .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", SPARK_ICEBERG_WAREHOUSE)
            )
        profile_conf = dict(profile.get("spark_conf") or {})
        requested_instances = _safe_int(profile_conf.get("spark.executor.instances"), 1, min_value=1)
        requested_cores = _safe_int(profile_conf.get("spark.executor.cores"), 1, min_value=1)
        requested_memory_mb = _memory_to_mb(profile_conf.get("spark.executor.memory"), 1024)
        capacity = _fetch_master_capacity_sync(master_ui_internal)
        capped_instances = requested_instances
        capped_cores = requested_cores
        capped_memory_mb = requested_memory_mb

        if capacity:
            total_cores = _safe_int(capacity.get("total_cores"), 0)
            total_memory_mb = _safe_int(capacity.get("total_memory_mb"), 0)
            if total_cores > 0:
                capped_cores = min(capped_cores, total_cores)
                max_instances_by_core = max(1, total_cores // max(1, capped_cores))
                capped_instances = min(capped_instances, max_instances_by_core)
            if total_memory_mb > 0:
                capped_memory_mb = min(capped_memory_mb, total_memory_mb)
                max_instances_by_mem = max(1, total_memory_mb // max(1, capped_memory_mb))
                capped_instances = min(capped_instances, max_instances_by_mem)

        profile_conf["spark.executor.instances"] = str(max(1, capped_instances))
        profile_conf["spark.executor.cores"] = str(max(1, capped_cores))
        profile_conf["spark.executor.memory"] = _format_memory_mb(max(1, capped_memory_mb))

        for conf_key, conf_val in profile_conf.items():
            k = str(conf_key).strip()
            v = str(conf_val).strip()
            if k and v:
                builder = builder.config(k, v)

        if SPARK_DRIVER_HOST:
            builder = builder.config("spark.driver.host", SPARK_DRIVER_HOST)
        if SPARK_DRIVER_BIND_ADDRESS:
            builder = builder.config("spark.driver.bindAddress", SPARK_DRIVER_BIND_ADDRESS)

        if not use_remote:
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

        if extra_packages and not use_remote:
            ordered_unique_packages = list(dict.fromkeys(extra_packages))
            builder = builder.config("spark.jars.packages", ",".join(ordered_unique_packages))

        try:
            spark_session = builder.create() if use_remote else builder.getOrCreate()
        except Exception as exc:
            if "stopped SparkContext" not in str(exc):
                raise
            logger.warning("Detected stopped SparkContext. Resetting and recreating Spark session.")
            _reset_spark_singletons()
            spark_session = builder.create() if use_remote else builder.getOrCreate()

        _spark_sessions[resolved_cluster_id] = spark_session
        runtime = _cluster_runtime(resolved_cluster_id)
        runtime["status"] = "running"
        runtime["started_at"] = _now_iso()
        runtime["last_used"] = runtime["started_at"]
        runtime["active_sessions"] = _count_sessions_for_cluster(resolved_cluster_id)
        runtime["last_error"] = None
        runtime["spark_ui_url"] = _spark_master_ui_url(master_url)
        runtime["available_resources"] = capacity
        runtime["effective_resources"] = _profile_resource_summary({"spark_conf": profile_conf})

        try:
            if not use_remote:
                runtime["spark_ui_url"] = spark_session.sparkContext.uiWebUrl or runtime["spark_ui_url"]
                runtime["application_id"] = spark_session.sparkContext.applicationId
            else:
                runtime["application_id"] = None
        except Exception:
            runtime["application_id"] = None

        logger.info(
            "Notebook Spark session initialized. cluster=%s mode=%s master=%s remote=%s app=%s",
            resolved_cluster_id,
            _spark_runtime_mode(resolved_cluster_id),
            master_url,
            remote_url or "-",
            app_name,
        )
        return spark_session


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

    # Spark Connect DataFrame type does not subclass pyspark.sql.DataFrame.
    try:
        module_name = type(value).__module__
        if module_name.startswith("pyspark.sql.connect") and hasattr(value, "collect") and hasattr(value, "columns"):
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


def _ensure_local_session(session_id: str, cluster_id: Optional[str] = None) -> None:
    resolved_cluster_id = _resolve_cluster_id(cluster_id)
    profile = _cluster_profile(resolved_cluster_id)
    if session_id not in _sessions:
        now = _now_iso()
        _sessions[session_id] = {
            "kernel_id": session_id,
            "created_at": now,
            "last_used": now,
            "mode": "local",
            "cluster_id": resolved_cluster_id,
            "cluster_label": profile.get("label"),
        }
    else:
        current_cluster = str(_sessions[session_id].get("cluster_id") or "").strip().lower()
        if current_cluster not in _CLUSTER_PROFILES:
            _sessions[session_id]["cluster_id"] = resolved_cluster_id
            _sessions[session_id]["cluster_label"] = profile.get("label")
    if session_id not in _execution_counts:
        _execution_counts[session_id] = 0
    if session_id not in _local_namespaces:
        _local_namespaces[session_id] = {
            "__name__": "__main__",
            "__builtins__": __builtins__,
        }
    _touch_cluster(_session_cluster_id(session_id))


def _install_local_helpers(namespace: dict[str, Any], session_id: str) -> None:
    cluster_id = _session_cluster_id(session_id)
    if namespace.get("__openclaw_helpers_loaded__") and namespace.get("__openclaw_cluster_id__") == cluster_id:
        try:
            # Keep proxy semantics on refresh so spark.sql(...) always gets retry/catalog-rewrite behavior.
            namespace["spark"] = _SparkSessionProxy(cluster_id)
        except Exception:
            pass
        return

    cluster_limits = _cluster_profile(cluster_id).get("limits", {})
    default_limit = int(cluster_limits.get("max_rows", NOTEBOOK_TABLE_ROW_LIMIT))

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
        limit: Optional[int] = default_limit,
    ):
        _require_module("pyspark.sql", "pyspark")
        sql_text = _prepare_spark_sql_text(statement)
        if not sql_text:
            raise RuntimeError("Spark SQL statement is empty")

        target_db = str(database).strip() or SPARK_DEFAULT_DATABASE
        sql_candidates: list[str] = [sql_text]
        session_retry_done = False
        rewrites_generated = False
        last_exception: Optional[Exception] = None

        while sql_candidates:
            current_sql = sql_candidates.pop(0)
            try:
                spark = _get_spark_session(cluster_id)
                with _spark_query_lock:
                    spark.catalog.setCurrentDatabase(target_db)
                    df = spark.sql(current_sql)
                    if limit is not None and int(limit) > 0:
                        df = df.limit(int(limit))
                    return df
            except Exception as exc:
                last_exception = exc
                if not session_retry_done and _is_spark_session_closed_error(exc):
                    logger.warning(
                        "Spark session handle closed for cluster '%s' in query_spark; recreating session.",
                        cluster_id,
                    )
                    _invalidate_spark_session(cluster_id, reason=str(exc))
                    session_retry_done = True
                    sql_candidates.insert(0, current_sql)
                    continue
                if not rewrites_generated:
                    try:
                        spark = _get_spark_session(cluster_id)
                        rewrites = _catalog_rewrite_targets(current_sql, spark, cluster_id, exc)
                        if rewrites:
                            sql_candidates.extend(
                                [_rewrite_catalog_prefix(current_sql, old, target) for old, target in rewrites]
                            )
                            logger.info(
                                "Detected unresolved Spark catalog prefix; retrying with discovered catalogs. cluster=%s options=%s",
                                cluster_id,
                                [target for _, target in rewrites],
                            )
                            rewrites_generated = True
                            continue
                    except Exception:
                        pass
                continue

        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Spark query failed")

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
        limit: Optional[int] = default_limit,
    ):
        return query_spark(statement, database=database, limit=limit)

    spark_sql_module = _require_module("pyspark.sql", "pyspark")
    spark_functions = _require_module("pyspark.sql.functions", "pyspark")
    spark_types = _require_module("pyspark.sql.types", "pyspark")
    spark = _SparkSessionProxy(cluster_id)

    namespace["query_trino"] = query_trino
    namespace["query_spark"] = query_spark
    namespace["query"] = query
    namespace["sql"] = sql
    namespace["spark_sql"] = spark_sql
    namespace["spark"] = spark
    namespace["SparkSession"] = spark_sql_module.SparkSession
    namespace["F"] = spark_functions
    namespace["T"] = spark_types
    namespace["__openclaw_cluster_id__"] = cluster_id
    namespace["__openclaw_sql_default_engine__"] = NOTEBOOK_SQL_ENGINE
    namespace["__openclaw_helpers_loaded__"] = True


def _execute_trino_sql(raw_sql: str, execution_count: int) -> dict[str, Any]:
    outputs: list[dict[str, Any]] = []
    statements = _split_sql_statements(_strip_sql_magic_prefix(raw_sql))
    if not statements:
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
        for idx, statement in enumerate(statements):
            cur.execute(statement)

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
                    "text": f"Statement {idx + 1}/{len(statements)} executed successfully.\\n"
                    if len(statements) > 1
                    else "Statement executed successfully.\\n",
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


def _execute_spark_sql(
    raw_sql: str,
    execution_count: int,
    session_id: str,
    database: str = SPARK_DEFAULT_DATABASE,
) -> dict[str, Any]:
    outputs: list[dict[str, Any]] = []
    statements = _split_sql_statements(_prepare_spark_sql_text(raw_sql))
    if not statements:
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

    cluster_id = _session_cluster_id(session_id)
    max_rows = int(_cluster_profile(cluster_id).get("limits", {}).get("max_rows", NOTEBOOK_TABLE_ROW_LIMIT))
    target_db = str(database).strip() or SPARK_DEFAULT_DATABASE

    pending_statement_sets: list[list[str]] = [list(statements)]
    rewrites_generated = False
    session_retry_done = False
    last_exception: Optional[Exception] = None

    while pending_statement_sets:
        active_statements = pending_statement_sets.pop(0)
        try:
            spark = _get_spark_session(cluster_id)
            attempt_outputs: list[dict[str, Any]] = []
            with _spark_query_lock:
                spark.catalog.setCurrentDatabase(target_db)
                for idx, statement in enumerate(active_statements):
                    df = spark.sql(statement)
                    columns = [str(c) for c in df.columns]

                    if columns:
                        rows = df.limit(max_rows + 1).collect()
                        truncated = len(rows) > max_rows
                        rows = rows[:max_rows]
                        values = [[row[col] for col in columns] for row in rows]
                        attempt_outputs.append(_table_output(columns, values, execution_count))
                        if truncated:
                            attempt_outputs.append({
                                "output_type": "stream",
                                "name": "stdout",
                                "text": f"Result truncated to {max_rows} rows.\\n",
                            })
                    else:
                        attempt_outputs.append({
                            "output_type": "stream",
                            "name": "stdout",
                            "text": f"Statement {idx + 1}/{len(active_statements)} executed successfully.\\n"
                            if len(active_statements) > 1
                            else "Statement executed successfully.\\n",
                        })

            outputs = attempt_outputs
            return {
                "status": "ok",
                "execution_count": execution_count,
                "outputs": outputs,
            }
        except Exception as exc:
            last_exception = exc
            if not session_retry_done and _is_spark_session_closed_error(exc):
                logger.warning(
                    "Spark session handle closed for cluster '%s' in %%sql; recreating session.",
                    cluster_id,
                )
                _invalidate_spark_session(cluster_id, reason=str(exc))
                session_retry_done = True
                pending_statement_sets.insert(0, active_statements)
                continue
            if not rewrites_generated:
                try:
                    spark = _get_spark_session(cluster_id)
                    rewrite_targets = _catalog_rewrite_targets("\n".join(active_statements), spark, cluster_id, exc)
                    if rewrite_targets:
                        rewritten_sets: list[list[str]] = []
                        for old_catalog, target_catalog in rewrite_targets:
                            rewritten = [
                                _rewrite_catalog_prefix(statement, old_catalog, target_catalog)
                                for statement in active_statements
                            ]
                            rewritten_sets.append(rewritten)
                        if rewritten_sets:
                            pending_statement_sets.extend(rewritten_sets)
                            rewrites_generated = True
                            logger.info(
                                "Detected unresolved Spark catalog prefix in %%sql; retrying with discovered catalogs. cluster=%s options=%s",
                                cluster_id,
                                [target for _, target in rewrite_targets],
                            )
                            continue
                except Exception:
                    pass

    exc = last_exception or RuntimeError("Spark SQL execution failed")
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
        _install_local_helpers(namespace, session_id)
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
    magic, payload = _parse_cell_magic(code or "")

    if magic == "%%spark":
        sql_code = payload.strip()
        return _execute_spark_sql(sql_code, execution_count, session_id=session_id)

    if magic == "%%trino":
        sql_code = payload.strip()
        return _execute_trino_sql(sql_code, execution_count)

    if magic in {"%%sql", "%sql"}:
        sql_body = payload.strip()
        selected_engine = NOTEBOOK_SQL_ENGINE
        match = re.match(r"^(spark|trino)\b", sql_body, re.IGNORECASE)
        if match:
            selected_engine = match.group(1).lower()
            sql_body = sql_body[len(match.group(0)):].strip()

        if selected_engine == "spark":
            return _execute_spark_sql(sql_body, execution_count, session_id=session_id)
        return _execute_trino_sql(sql_body, execution_count)

    if magic in {"%python", "%%python"}:
        return _execute_local_python(payload, session_id, execution_count)

    if magic in {"%md", "%%md"}:
        return _render_markdown_output(payload, execution_count)

    if magic in {"%sh", "%%sh", "!"}:
        return _run_shell_command(payload, execution_count)

    if magic in {"%pip", "%%pip"}:
        return _run_pip_magic(payload, execution_count)

    return _execute_local_python(code, session_id, execution_count)


def _session_public(session_id: str, info: dict[str, Any]) -> dict[str, Any]:
    cluster_id = str(info.get("cluster_id") or _DEFAULT_CLUSTER_ID).strip().lower()
    if cluster_id not in _CLUSTER_PROFILES:
        cluster_id = _DEFAULT_CLUSTER_ID
    cluster = _cluster_profile(cluster_id)
    return {
        "id": session_id,
        "name": "python3",
        "execution_state": "idle",
        **info,
        "cluster_id": cluster_id,
        "cluster_label": cluster.get("label"),
    }


def _cluster_payload(cluster_id: str) -> dict[str, Any]:
    profile = _cluster_profile(cluster_id)
    runtime = _cluster_runtime(cluster_id)
    runtime["active_sessions"] = _count_sessions_for_cluster(cluster_id)
    profile_resources = _profile_resource_summary(profile)
    available_resources = runtime.get("available_resources") if isinstance(runtime.get("available_resources"), dict) else None
    if isinstance(runtime.get("effective_resources"), dict):
        effective_resources = _cap_profile_resources_to_available(runtime.get("effective_resources"), available_resources)
    else:
        effective_resources = _cap_profile_resources_to_available(profile_resources, available_resources)
    shuffle_tuning = _shuffle_tuning_from_resources(effective_resources)
    return {
        "id": cluster_id,
        "label": profile.get("label"),
        "description": profile.get("description"),
        "runtime_mode": _spark_runtime_mode(cluster_id),
        "spark_master_url": _cluster_master_url(cluster_id),
        "spark_remote_url": _cluster_remote_url(cluster_id) or None,
        "spark_conf": dict(profile.get("spark_conf") or {}),
        "limits": dict(profile.get("limits") or {}),
        "resources": profile_resources,
        "effective_resources": effective_resources,
        "available_resources": available_resources,
        "shuffle_tuning": shuffle_tuning,
        "runtime": {
            "status": runtime.get("status"),
            "active_sessions": runtime.get("active_sessions", 0),
            "last_used": runtime.get("last_used"),
            "started_at": runtime.get("started_at"),
            "last_stopped": runtime.get("last_stopped"),
            "auto_stops": runtime.get("auto_stops", 0),
            "spark_ui_url": runtime.get("spark_ui_url"),
            "application_id": runtime.get("application_id"),
            "last_error": runtime.get("last_error"),
        },
    }


async def _fetch_json(url: str, timeout_seconds: float = 2.5) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.get(url)
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}")
        return response.json() if response.text else {}


async def _fetch_cluster_observability(cluster_id: str) -> dict[str, Any]:
    runtime = _cluster_runtime(cluster_id)
    master_ui_internal = _spark_master_ui_url(_cluster_master_url(cluster_id), public=False)
    master_ui_public = _spark_master_ui_url(_cluster_master_url(cluster_id), public=True)
    spark_ui_url = runtime.get("spark_ui_url") or master_ui_public

    workers_summary = {
        "count": 0,
        "cores_total": 0,
        "cores_used": 0,
        "memory_total_mb": 0,
        "memory_used_mb": 0,
    }
    active_apps: list[dict[str, Any]] = []
    app_logs: list[dict[str, Any]] = []

    if master_ui_internal:
        try:
            master_json = await _fetch_json(f"{master_ui_internal.rstrip('/')}/json/")
            workers = master_json.get("workers") if isinstance(master_json.get("workers"), list) else []
            for worker in workers:
                workers_summary["count"] += 1
                workers_summary["cores_total"] += int(worker.get("cores", 0) or 0)
                workers_summary["cores_used"] += int(worker.get("coresused", 0) or 0)
                workers_summary["memory_total_mb"] += int(worker.get("memory", 0) or 0)
                workers_summary["memory_used_mb"] += int(worker.get("memoryused", 0) or 0)

            raw_apps = master_json.get("activeapps") if isinstance(master_json.get("activeapps"), list) else []
            for app in raw_apps:
                app_name = str(app.get("name") or "")
                if f"-{cluster_id}" not in app_name and cluster_id != _DEFAULT_CLUSTER_ID:
                    continue
                app_id = str(app.get("id") or "")
                app_ui = str(app.get("webuiaddress") or "").rstrip("/")
                app_ui_public = app_ui
                if master_ui_internal and master_ui_public and app_ui.startswith(master_ui_internal):
                    app_ui_public = f"{master_ui_public}{app_ui[len(master_ui_internal):]}"
                active_apps.append(
                    {
                        "id": app_id,
                        "name": app_name,
                        "cores_granted": int(app.get("coresgranted", 0) or 0),
                        "memory_per_executor_mb": int(app.get("memoryperexecutor", 0) or 0),
                        "executors": int(app.get("executors", 0) or 0),
                        "spark_ui_url": app_ui_public or spark_ui_url,
                    }
                )
                if app_id and app_ui:
                    try:
                        jobs = await _fetch_json(f"{app_ui}/api/v1/applications/{app_id}/jobs?status=running", timeout_seconds=1.5)
                        stages = await _fetch_json(
                            f"{app_ui}/api/v1/applications/{app_id}/stages?status=active",
                            timeout_seconds=1.5,
                        )
                        running_jobs = jobs if isinstance(jobs, list) else []
                        active_stages = stages if isinstance(stages, list) else []
                        app_logs.append(
                            {
                                "application_id": app_id,
                                "running_jobs": len(running_jobs),
                                "active_stages": len(active_stages),
                                "jobs": [
                                    {
                                        "job_id": j.get("jobId"),
                                        "name": j.get("name"),
                                        "status": j.get("status"),
                                    }
                                    for j in running_jobs[:20]
                                    if isinstance(j, dict)
                                ],
                                "stages": [
                                    {
                                        "stage_id": s.get("stageId"),
                                        "name": s.get("name"),
                                        "status": s.get("status"),
                                        "num_tasks": s.get("numTasks"),
                                        "num_active_tasks": s.get("numActiveTasks"),
                                    }
                                    for s in active_stages[:20]
                                    if isinstance(s, dict)
                                ],
                            }
                        )
                    except Exception:
                        pass
        except Exception as exc:
            runtime["last_error"] = f"observability_fetch_failed: {exc}"

    runtime["spark_ui_url"] = spark_ui_url
    runtime["active_sessions"] = _count_sessions_for_cluster(cluster_id)
    runtime["status"] = "running" if runtime["active_sessions"] > 0 else runtime.get("status", "idle")
    runtime["available_resources"] = {
        "workers": workers_summary.get("count", 0),
        "total_cores": workers_summary.get("cores_total", 0),
        "total_memory_mb": workers_summary.get("memory_total_mb", 0),
        "total_memory": _format_memory_mb(int(workers_summary.get("memory_total_mb", 0) or 0)),
    }

    return {
        "cluster": _cluster_payload(cluster_id),
        "spark_ui_url": spark_ui_url,
        "spark_master_ui_url": master_ui_public,
        "workers": workers_summary,
        "active_applications": active_apps,
        "logs": app_logs,
    }


async def _refresh_cluster_capacities() -> None:
    for cluster_id in _CLUSTER_PROFILES.keys():
        runtime = _cluster_runtime(cluster_id)
        master_internal = _spark_master_ui_url(_cluster_master_url(cluster_id), public=False)
        runtime["available_resources"] = await _fetch_master_capacity_async(master_internal)


# ─── Routes: Kernel Management ───

@router.get("/notebook/clusters")
async def list_notebook_clusters():
    """List available notebook cluster profiles with runtime state."""
    await _cleanup_stale_sessions()
    await _refresh_cluster_capacities()
    return {
        "default_cluster_id": _DEFAULT_CLUSTER_ID,
        "clusters": [_cluster_payload(cluster_id) for cluster_id in _CLUSTER_PROFILES.keys()],
    }


@router.get("/notebook/observability")
async def notebook_observability(cluster_id: Optional[str] = None):
    """Return Spark observability (workers/apps/jobs/stages + Spark UI links)."""
    await _cleanup_stale_sessions()
    if cluster_id:
        resolved = _resolve_cluster_id(cluster_id)
        return {
            "generated_at": _now_iso(),
            "clusters": [await _fetch_cluster_observability(resolved)],
        }
    clusters = []
    for cluster_key in _CLUSTER_PROFILES.keys():
        clusters.append(await _fetch_cluster_observability(cluster_key))
    return {
        "generated_at": _now_iso(),
        "clusters": clusters,
    }


@router.get("/notebook/clusters/{cluster_id}/observability")
async def notebook_cluster_observability(cluster_id: str):
    """Return observability for one cluster profile."""
    await _cleanup_stale_sessions()
    resolved = _resolve_cluster_id(cluster_id)
    return await _fetch_cluster_observability(resolved)


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
        "kernels": [_session_public(sid, info) for sid, info in _sessions.items()],
        "mode": "local",
    }


@router.post("/notebook/kernels")
async def start_kernel(req: Optional[StartKernelRequest] = None):
    """Start a new kernel session."""
    await _cleanup_stale_sessions()
    requested_cluster_id = _resolve_cluster_id(req.cluster_id if req else None)
    profile = _cluster_profile(requested_cluster_id)
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
                "cluster_id": requested_cluster_id,
                "cluster_label": profile.get("label"),
            }
            _touch_cluster(requested_cluster_id)
            return {
                "id": session_id,
                "mode": "gateway",
                "status": "started",
                "cluster_id": requested_cluster_id,
                "cluster_label": profile.get("label"),
            }
        except Exception as e:
            logger.warning("Kernel Gateway unavailable, falling back to local: %s", e)

    session_id = str(uuid.uuid4())
    _ensure_local_session(session_id, cluster_id=requested_cluster_id)
    return {
        "id": session_id,
        "mode": "local",
        "status": "started",
        "cluster_id": requested_cluster_id,
        "cluster_label": profile.get("label"),
    }


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

    await asyncio.to_thread(_cleanup_idle_spark_sessions)
    return {
        "status": "shutdown",
        "id": kernel_id,
        "cluster_id": session.get("cluster_id") if isinstance(session, dict) else None,
    }


@router.post("/notebook/kernels/{kernel_id}/restart")
async def restart_kernel(kernel_id: str):
    """Restart a kernel session."""
    session = _sessions.get(kernel_id)
    if session and session.get("mode") == "gateway":
        try:
            await _kg_request("POST", f"/api/kernels/{session['kernel_id']}/restart")
            _touch_session(kernel_id)
            return {
                "status": "restarted",
                "mode": "gateway",
                "cluster_id": session.get("cluster_id") or _DEFAULT_CLUSTER_ID,
            }
        except Exception:
            pass

    target_cluster = session.get("cluster_id") if isinstance(session, dict) else None
    _ensure_local_session(kernel_id, cluster_id=target_cluster)
    _execution_counts[kernel_id] = 0
    _local_namespaces[kernel_id] = {
        "__name__": "__main__",
        "__builtins__": __builtins__,
    }
    _touch_session(kernel_id)
    return {
        "status": "restarted",
        "mode": "local",
        "cluster_id": _session_cluster_id(kernel_id),
    }


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


@router.post("/notebook/kernels/{kernel_id}/attach")
async def attach_cluster(kernel_id: str, req: AttachClusterRequest):
    """Attach an existing notebook kernel to a cluster profile."""
    await _cleanup_stale_sessions()
    target_cluster_id = _resolve_cluster_id(req.cluster_id)
    profile = _cluster_profile(target_cluster_id)

    _ensure_local_session(kernel_id, cluster_id=target_cluster_id)
    session = _sessions[kernel_id]
    session["cluster_id"] = target_cluster_id
    session["cluster_label"] = profile.get("label")
    _touch_session(kernel_id)

    if req.restart:
        _execution_counts[kernel_id] = 0
        _local_namespaces[kernel_id] = {
            "__name__": "__main__",
            "__builtins__": __builtins__,
        }

    await asyncio.to_thread(_cleanup_idle_spark_sessions)
    return {
        "status": "attached",
        "id": kernel_id,
        "cluster_id": target_cluster_id,
        "cluster_label": profile.get("label"),
        "restarted": bool(req.restart),
    }


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

    _ensure_local_session(kernel_id, cluster_id=session.get("cluster_id") if isinstance(session, dict) else None)
    lock = _session_locks.setdefault(kernel_id, asyncio.Lock())
    async with lock:
        result = await asyncio.to_thread(_execute_local, req.code, kernel_id)
    _touch_session(kernel_id)
    result["cluster_id"] = _session_cluster_id(kernel_id)
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
            source_text = "".join(cell.get("source", []))
            first_non_empty = ""
            for raw_line in source_text.splitlines():
                stripped_line = raw_line.strip()
                if stripped_line:
                    first_non_empty = stripped_line.lower()
                    break

            metadata = cell.get("metadata") if isinstance(cell.get("metadata"), dict) else {}
            metadata_lang = str(metadata.get("language") or metadata.get("language_id") or "").strip().lower()

            detected_language = "python"
            if first_non_empty.startswith("%%sql") or first_non_empty.startswith("%sql") or metadata_lang == "sql":
                detected_language = "sql"

            cells.append({
                "id": str(uuid.uuid4()),
                "cell_type": cell.get("cell_type", "code"),
                "source": source_text,
                "language": detected_language,
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
    await _refresh_cluster_capacities()
    gw_available = await _kernel_gw_available()
    clusters = [_cluster_payload(cluster_id) for cluster_id in _CLUSTER_PROFILES.keys()]
    return {
        "gateway_available": gw_available,
        "gateway_enabled": NOTEBOOK_USE_GATEWAY,
        "gateway_url": KERNEL_GW_URL,
        "mode": "gateway" if gw_available else "local",
        "spark_runtime_mode": _spark_runtime_mode(_DEFAULT_CLUSTER_ID),
        "active_sessions": len(_sessions),
        "cleaned_sessions": cleaned,
        "session_ttl_seconds": NOTEBOOK_SESSION_TTL_SECONDS,
        "cluster_idle_timeout_seconds": NOTEBOOK_CLUSTER_IDLE_TIMEOUT_SECONDS,
        "default_cluster_id": _DEFAULT_CLUSTER_ID,
        "clusters": clusters,
        "sql_default_engine": NOTEBOOK_SQL_ENGINE,
        "spark_master_url": SPARK_MASTER_URL,
        "spark_remote_url": SPARK_REMOTE_URL or None,
        "spark_default_database": SPARK_DEFAULT_DATABASE,
        "notebooks_dir": str(NOTEBOOKS_DIR),
    }
