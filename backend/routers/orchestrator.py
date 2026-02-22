"""
Orchestrator Router — Airflow REST API client with JWT auth.
Ported from:
  - web-ui/src/lib/airflow/auth.ts (JWT token manager)
  - web-ui/src/lib/airflow/client.ts (API client)  
  - web-ui/src/app/api/orchestrator/* (7 route files)
"""
import asyncio
import base64
import json
import logging
import os
import re
import time
from contextvars import ContextVar
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Optional, Literal
from urllib.parse import urlencode
import httpx

logger = logging.getLogger(__name__)

_request_airflow_token: ContextVar[Optional[str]] = ContextVar(
    "request_airflow_token", default=None
)


async def bind_request_airflow_token(request: Request) -> None:
    header = request.headers.get("X-Airflow-Authorization") or request.headers.get("Authorization")
    token: Optional[str] = None
    if header and header.lower().startswith("bearer "):
        token = header.split(" ", 1)[1].strip()
    _request_airflow_token.set(token)


router = APIRouter(dependencies=[Depends(bind_request_airflow_token)])

AIRFLOW_BASE = os.getenv("AIRFLOW_API_URL", "http://localhost:8081")
API_PREFIX = "/api/v2"
SA_USERNAME = os.getenv("AIRFLOW_SA_USERNAME", "admin")
SA_PASSWORD = os.getenv("AIRFLOW_SA_PASSWORD", "admin")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NOTEBOOKS_DIR = Path(
    os.getenv(
        "ORCH_NOTEBOOKS_DIR",
        "/opt/airflow/notebooks" if Path("/opt/airflow/notebooks").exists() else str(PROJECT_ROOT / "notebooks"),
    )
)
DAGS_DIR = Path(
    os.getenv(
        "ORCH_DAGS_DIR",
        "/opt/airflow/dags" if Path("/opt/airflow/dags").exists() else str(PROJECT_ROOT / "dags"),
    )
)
DOCKER_NOTEBOOK_PREFIX = "/opt/airflow/notebooks/"

# ─── JWT Token Manager ───

_cached_token: Optional[str] = None
_token_expires_at: float = 0

_last_run_cache: dict[str, tuple[float, Optional[dict]]] = {}
LAST_RUN_CACHE_TTL_SECONDS = int(os.getenv("ORCH_LAST_RUN_CACHE_TTL_SECONDS", "30"))
LAST_RUN_FETCH_CONCURRENCY = int(os.getenv("ORCH_LAST_RUN_FETCH_CONCURRENCY", "8"))


async def get_airflow_token() -> str:
    """Authenticate with Airflow and cache the JWT token."""
    global _cached_token, _token_expires_at

    # Return cached token if still valid (with 60s buffer)
    if _cached_token and time.time() < _token_expires_at - 60:
        return _cached_token

    async with httpx.AsyncClient(timeout=10.0) as client:
        res = await client.post(
            f"{AIRFLOW_BASE}/auth/token",
            json={"username": SA_USERNAME, "password": SA_PASSWORD},
        )

        if res.status_code not in (200, 201):
            raise HTTPException(
                status_code=502,
                detail=f"Airflow auth failed ({res.status_code}): {res.text}",
            )

        data = res.json()
        _cached_token = data["access_token"]

        # Parse JWT to get expiry
        try:
            payload_b64 = _cached_token.split(".")[1]
            # Add padding if needed
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.b64decode(payload_b64))
            _token_expires_at = payload.get("exp", time.time() + 86400)
        except Exception:
            _token_expires_at = time.time() + 86400  # 24h fallback

        return _cached_token


# ─── Core Fetch Helper ───


async def airflow_fetch(
    path: str,
    method: str = "GET",
    body: dict = None,
    extra_headers: Optional[dict] = None,
) -> dict:
    """Authenticated request to Airflow REST API."""
    token = _request_airflow_token.get() or await get_airflow_token()
    url = f"{AIRFLOW_BASE}{API_PREFIX}{path}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)

        kwargs = {"headers": headers}
        if body is not None:
            kwargs["json"] = body

        if method == "GET":
            res = await client.get(url, **kwargs)
        elif method == "POST":
            res = await client.post(url, **kwargs)
        elif method == "PATCH":
            res = await client.patch(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")

    if not res.is_success:
        err_msg = f"Airflow {res.status_code}"
        try:
            err_body = res.json()
            if "detail" in err_body:
                if isinstance(err_body["detail"], list):
                    err_msg += ": " + "; ".join(
                        d.get("msg", json.dumps(d)) for d in err_body["detail"]
                    )
                else:
                    err_msg += ": " + str(err_body["detail"])
            else:
                err_msg += ": " + json.dumps(err_body)
        except Exception:
            err_msg += ": " + res.text
        raise HTTPException(status_code=res.status_code, detail=err_msg)

    content_type = res.headers.get("content-type", "")
    if "application/json" in content_type:
        return res.json()
    return {"text": res.text}


# ─── Transform Helpers ───


def transform_dag(dag: dict, last_run: Optional[dict] = None) -> dict:
    return {
        "dag_id": dag.get("dag_id"),
        "display_name": dag.get("dag_display_name"),
        "description": dag.get("description"),
        "is_paused": dag.get("is_paused"),
        "schedule": dag.get("timetable_summary"),
        "schedule_description": dag.get("timetable_description"),
        "owners": dag.get("owners", []),
        "tags": [t["name"] for t in dag.get("tags", [])],
        "next_run": dag.get("next_dagrun_logical_date"),
        "max_active_runs": dag.get("max_active_runs"),
        "file_path": dag.get("relative_fileloc"),
        "has_import_errors": dag.get("has_import_errors"),
        "last_run": last_run,
    }


def transform_run(run: dict) -> dict:
    duration_seconds = None
    if run.get("start_date") and run.get("end_date"):
        from datetime import datetime

        try:
            start = datetime.fromisoformat(run["start_date"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(run["end_date"].replace("Z", "+00:00"))
            duration_seconds = (end - start).total_seconds()
        except Exception:
            pass

    return {
        "run_id": run.get("dag_run_id"),
        "logical_date": run.get("logical_date"),
        "run_after": run.get("run_after"),
        "state": run.get("state"),
        "run_type": run.get("run_type"),
        "triggered_by": run.get("triggered_by"),
        "queued_at": run.get("queued_at"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "duration_seconds": duration_seconds,
        "conf": run.get("conf"),
        "note": run.get("note"),
    }


# ─── API Functions ───


async def list_dags(limit=100, offset=0, dag_id_pattern=None, paused=None, tags=None):
    params = f"limit={limit}&offset={offset}"
    if paused is not None:
        params += f"&paused={str(paused).lower()}"
    if dag_id_pattern:
        params += f"&dag_id_pattern={dag_id_pattern}"
    if tags:
        for t in tags:
            params += f"&tags={t}"

    data = await airflow_fetch(f"/dags?{params}")

    # Fetch last run for each DAG with bounded concurrency.
    semaphore = asyncio.Semaphore(max(1, LAST_RUN_FETCH_CONCURRENCY))

    async def get_dag_with_run(dag):
        async with semaphore:
            last_run = await get_cached_last_run(dag["dag_id"])
            return transform_dag(dag, last_run)

    dags_with_runs = await asyncio.gather(
        *[get_dag_with_run(dag) for dag in data.get("dags", [])]
    )

    return {"dags": dags_with_runs, "total": data.get("total_entries", 0)}


async def get_dag(dag_id: str):
    dag = await airflow_fetch(f"/dags/{dag_id}")
    last_run = await get_cached_last_run(dag_id)
    return transform_dag(dag, last_run)


async def list_runs(
    dag_id: str,
    limit=25,
    offset=0,
    state: Optional[list[str]] = None,
    run_type: Optional[list[str]] = None,
    order_by: str = "-start_date",
    run_after_gte: Optional[str] = None,
    run_after_lte: Optional[str] = None,
    logical_date_gte: Optional[str] = None,
    logical_date_lte: Optional[str] = None,
    start_date_gte: Optional[str] = None,
    start_date_lte: Optional[str] = None,
    end_date_gte: Optional[str] = None,
    end_date_lte: Optional[str] = None,
):
    query_pairs: list[tuple[str, str]] = [
        ("order_by", order_by),
        ("limit", str(limit)),
        ("offset", str(offset)),
    ]

    if state:
        for item in state:
            query_pairs.append(("state", item))
    if run_type:
        for item in run_type:
            query_pairs.append(("run_type", item))

    optional_scalar = {
        "run_after_gte": run_after_gte,
        "run_after_lte": run_after_lte,
        "logical_date_gte": logical_date_gte,
        "logical_date_lte": logical_date_lte,
        "start_date_gte": start_date_gte,
        "start_date_lte": start_date_lte,
        "end_date_gte": end_date_gte,
        "end_date_lte": end_date_lte,
    }
    for key, value in optional_scalar.items():
        if value:
            query_pairs.append((key, value))

    params = urlencode(query_pairs)
    data = await airflow_fetch(f"/dags/{dag_id}/dagRuns?{params}")
    return {
        "runs": [transform_run(r) for r in data.get("dag_runs", [])],
        "total": data.get("total_entries", 0),
    }


async def trigger_run(dag_id: str, conf=None):
    from datetime import datetime, timezone

    result = await airflow_fetch(
        f"/dags/{dag_id}/dagRuns",
        method="POST",
        body={
            "logical_date": datetime.now(timezone.utc).isoformat(),
            "conf": conf or {},
        },
    )
    invalidate_last_run_cache(dag_id)
    return transform_run(result)


async def cancel_run(dag_id: str, run_id: str):
    result = await airflow_fetch(
        f"/dags/{dag_id}/dagRuns/{run_id}",
        method="PATCH",
        body={"state": "failed"},
    )
    invalidate_last_run_cache(dag_id)
    return transform_run(result)


async def toggle_dag_pause(dag_id: str, is_paused: bool):
    result = await airflow_fetch(
        f"/dags/{dag_id}", method="PATCH", body={"is_paused": is_paused}
    )
    return {"dag_id": result.get("dag_id"), "is_paused": result.get("is_paused")}


async def get_dag_source(dag_id: str):
    result = await airflow_fetch(f"/dagSources/{dag_id}")
    return {"content": result.get("content"), "version": result.get("version_number")}


async def get_dag_structure(dag_id: str):
    data = await airflow_fetch(f"/dags/{dag_id}/tasks")
    nodes = [
        {
            "id": t.get("task_id"),
            "label": t.get("task_display_name"),
            "operator": t.get("operator_name"),
            "color": t.get("ui_color"),
            "fgColor": t.get("ui_fgcolor"),
            "triggerRule": t.get("trigger_rule"),
            "isMapped": t.get("is_mapped"),
        }
        for t in data.get("tasks", [])
    ]
    edges = []
    for task in data.get("tasks", []):
        for downstream in task.get("downstream_task_ids", []):
            edges.append({"source": task["task_id"], "target": downstream})
    return {"nodes": nodes, "edges": edges}


async def get_task_instances(dag_id: str, run_id: str):
    data = await airflow_fetch(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    return data.get("task_instances", [])


async def get_health():
    data = await airflow_fetch("/monitor/health")
    return {
        "scheduler": {
            "status": data.get("scheduler", {}).get("status", "unknown"),
            "heartbeat": data.get("scheduler", {}).get("latest_scheduler_heartbeat"),
        },
        "database": {"status": data.get("metadatabase", {}).get("status", "unknown")},
        "dag_processor": {
            "status": data.get("dag_processor", {}).get("status", "unknown"),
            "heartbeat": data.get("dag_processor", {}).get("latest_dag_processor_heartbeat"),
        },
    }

async def clear_task_instances(
    dag_id: str,
    run_id: str,
    task_id: str,
    include_upstream: bool = False,
    include_downstream: bool = False,
    include_future: bool = False,
    include_past: bool = False,
):
    body = {
        "dry_run": False,
        "task_ids": [task_id],
        "dag_run_id": run_id,
        "include_upstream": include_upstream,
        "include_downstream": include_downstream,
        "include_future": include_future,
        "include_past": include_past,
    }
    result = await airflow_fetch(f"/dags/{dag_id}/clearTaskInstances", method="POST", body=body)
    return result

async def patch_task_instance_state(
    dag_id: str,
    run_id: str,
    task_id: str,
    new_state: str,
    map_index: int = -1,
    note: Optional[str] = None,
    include_upstream: bool = False,
    include_downstream: bool = False,
    include_future: bool = False,
    include_past: bool = False,
):
    query_params = {}
    if map_index != -1:
        query_params["map_index"] = map_index

    body = {
        "new_state": new_state,
        "include_upstream": include_upstream,
        "include_downstream": include_downstream,
        "include_future": include_future,
        "include_past": include_past,
    }
    if note:
        body["note"] = note

    path = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
    if query_params:
        path += f"?{urlencode(query_params)}"

    return await airflow_fetch(path, method="PATCH", body=body)


async def get_task_instance_tries(
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = -1,
):
    query_params = {}
    if map_index != -1:
        query_params["map_index"] = map_index

    path = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/tries"
    if query_params:
        path += f"?{urlencode(query_params)}"

    return await airflow_fetch(path)


async def get_task_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    map_index: int = -1,
    full_content: bool = True,
    continuation_token: Optional[str] = None,
):
    query_params = {"full_content": str(full_content).lower()}
    if map_index != -1:
        query_params["map_index"] = map_index
    if continuation_token:
        query_params["token"] = continuation_token

    path = (
        f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        f"?{urlencode(query_params)}"
    )
    result = await airflow_fetch(path, extra_headers={"accept": "application/json"})

    content = result.get("content")
    continuation = result.get("continuation_token")

    if isinstance(content, list):
        rendered_lines = []
        for item in content:
            if isinstance(item, str):
                rendered_lines.append(item)
            elif isinstance(item, dict):
                rendered_lines.append(
                    str(item.get("event") or item.get("message") or json.dumps(item))
                )
            else:
                rendered_lines.append(str(item))
        rendered = "".join(rendered_lines)
    elif content is None:
        rendered = result.get("text", "")
    else:
        rendered = str(content)

    return {
        "content": rendered,
        "continuation_token": continuation,
    }


def invalidate_last_run_cache(dag_id: str) -> None:
    _last_run_cache.pop(dag_id, None)


async def get_cached_last_run(dag_id: str) -> Optional[dict]:
    now = time.time()
    cached = _last_run_cache.get(dag_id)
    if cached and (now - cached[0]) < LAST_RUN_CACHE_TTL_SECONDS:
        return cached[1]

    last_run = None
    try:
        runs = await airflow_fetch(f"/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1")
        if runs.get("dag_runs") and len(runs["dag_runs"]) > 0:
            last_run = transform_run(runs["dag_runs"][0])
    except Exception:
        last_run = None

    _last_run_cache[dag_id] = (now, last_run)
    return last_run


# ─── Notebook Resolution ───


def build_task_notebook_map(dag_id: Optional[str] = None) -> dict:
    """Parse DAG files to build task_id → notebook path mapping."""
    mapping = {}
    if not DAGS_DIR.exists():
        return mapping

    dag_files = []
    if dag_id:
        candidate = DAGS_DIR / f"{dag_id}.py"
        if candidate.exists():
            dag_files.append(candidate)

    if not dag_files:
        dag_files = list(DAGS_DIR.glob("*.py"))

    for dag_file in dag_files:
        content = dag_file.read_text(encoding="utf-8", errors="replace")
        for match in re.finditer(r"SparkSubmitOperator\s*\(([\s\S]*?)\)", content):
            block = match.group(1)
            task_match = re.search(r"task_id\s*=\s*['\"]([^'\"]+)['\"]", block)
            app_match = re.search(r"application\s*=\s*['\"]([^'\"]+)['\"]", block)
            if task_match and app_match:
                task_id = task_match.group(1)
                app_path = app_match.group(1)
                if app_path.startswith(DOCKER_NOTEBOOK_PREFIX):
                    app_path = app_path[len(DOCKER_NOTEBOOK_PREFIX):]
                mapping[task_id] = app_path

    return mapping


def scan_notebooks(directory: Path, prefix: str = "") -> dict:
    """Recursively scan notebooks directory for .py files."""
    result = {}
    if not directory.exists():
        return result

    for entry in directory.iterdir():
        if entry.name.startswith(("_", ".")) or entry.name == "__pycache__":
            continue
        relative = f"{prefix}/{entry.name}" if prefix else entry.name
        if entry.is_dir():
            result.update(scan_notebooks(entry, relative))
        elif entry.suffix == ".py":
            result[entry.name] = relative
            result[entry.stem] = relative

    return result


# ─── Route Models ───


class DagActionRequest(BaseModel):
    dag_id: str
    action: str
    conf: Optional[dict] = None
    run_id: Optional[str] = None


class NotebookRequest(BaseModel):
    task_id: str
    dag_id: Optional[str] = None

class TaskActionRequest(BaseModel):
    action: Literal["clear", "mark_success", "mark_failed", "mark_running", "mark_queued", "set_state"]
    new_state: Optional[str] = None
    note: Optional[str] = None
    include_upstream: bool = False
    include_downstream: bool = False
    include_future: bool = False
    include_past: bool = False
    map_index: int = -1


# ─── Routes ───


# GET /api/orchestrator/dags
@router.get("/orchestrator/dags")
async def get_dags(
    limit: int = Query(default=100),
    offset: int = Query(default=0),
    search: Optional[str] = Query(default=None),
    paused: Optional[bool] = Query(default=None),
):
    try:
        result = await list_dags(
            limit=limit, offset=offset, dag_id_pattern=search, paused=paused
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /dags error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# POST /api/orchestrator/dags
@router.post("/orchestrator/dags")
async def post_dags(request: DagActionRequest):
    try:
        if request.action == "trigger":
            run = await trigger_run(request.dag_id, request.conf)
            return {"message": "Run triggered", "run": run}

        if request.action == "pause":
            result = await toggle_dag_pause(request.dag_id, True)
            return {"message": "DAG paused", **result}

        if request.action == "unpause":
            result = await toggle_dag_pause(request.dag_id, False)
            return {"message": "DAG unpaused", **result}

        if request.action == "cancel":
            if not request.run_id:
                raise HTTPException(status_code=400, detail="Missing run_id for cancel")
            result = await cancel_run(request.dag_id, request.run_id)
            return {"message": "Run cancelled", "run": result}

        if request.action == "source":
            result = await get_dag_source(request.dag_id)
            return result

        raise HTTPException(status_code=400, detail="Invalid action")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] POST /dags error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# GET /api/orchestrator/dags/{dag_id}
@router.get("/orchestrator/dags/{dag_id}")
async def get_dag_detail(
    dag_id: str,
    include_runs: bool = Query(default=False),
    runs_limit: int = Query(default=25),
):
    try:
        dag = await get_dag(dag_id)
        runs = None
        if include_runs:
            result = await list_runs(dag_id, limit=runs_limit)
            runs = result["runs"]
        return {"dag": dag, "runs": runs}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /dags/%s error: %s", dag_id, e)
        raise HTTPException(status_code=500, detail=str(e))


# GET /api/orchestrator/dags/{dag_id}/runs
@router.get("/orchestrator/dags/{dag_id}/runs")
async def get_dag_runs(
    dag_id: str,
    limit: int = Query(default=25),
    offset: int = Query(default=0),
    state: Optional[str] = Query(default=None),
    run_type: Optional[str] = Query(default=None),
    triggered_by: Optional[str] = Query(default=None),
    order_by: str = Query(default="-start_date"),
    run_after_gte: Optional[str] = Query(default=None),
    run_after_lte: Optional[str] = Query(default=None),
    logical_date_gte: Optional[str] = Query(default=None),
    logical_date_lte: Optional[str] = Query(default=None),
    start_date_gte: Optional[str] = Query(default=None),
    start_date_lte: Optional[str] = Query(default=None),
    end_date_gte: Optional[str] = Query(default=None),
    end_date_lte: Optional[str] = Query(default=None),
):
    try:
        parsed_state = [item.strip() for item in state.split(",")] if state else None
        parsed_run_type = [item.strip() for item in run_type.split(",")] if run_type else None
        triggered_by_filter = (triggered_by or "").strip().lower()

        if triggered_by_filter:
            # Airflow API does not consistently support triggered_by filtering.
            # Scan pages server-side, then apply filter and paginate deterministically.
            scan_limit = max(100, min(500, max(limit + offset, 100)))
            scan_offset = 0
            max_scan_rows = 5000
            matched_runs: list[dict] = []
            total_entries = 0

            while scan_offset < max_scan_rows:
                page = await list_runs(
                    dag_id,
                    limit=scan_limit,
                    offset=scan_offset,
                    state=parsed_state,
                    run_type=parsed_run_type,
                    order_by=order_by,
                    run_after_gte=run_after_gte,
                    run_after_lte=run_after_lte,
                    logical_date_gte=logical_date_gte,
                    logical_date_lte=logical_date_lte,
                    start_date_gte=start_date_gte,
                    start_date_lte=start_date_lte,
                    end_date_gte=end_date_gte,
                    end_date_lte=end_date_lte,
                )
                total_entries = int(page.get("total", 0) or 0)
                page_runs = page.get("runs", [])
                if not page_runs:
                    break

                matched_runs.extend(
                    [
                        run
                        for run in page_runs
                        if triggered_by_filter in str(run.get("triggered_by") or "").lower()
                    ]
                )

                scan_offset += len(page_runs)
                if scan_offset >= total_entries:
                    break

            return {
                "runs": matched_runs[offset: offset + limit],
                "total": len(matched_runs),
            }

        return await list_runs(
            dag_id,
            limit=limit,
            offset=offset,
            state=parsed_state,
            run_type=parsed_run_type,
            order_by=order_by,
            run_after_gte=run_after_gte,
            run_after_lte=run_after_lte,
            logical_date_gte=logical_date_gte,
            logical_date_lte=logical_date_lte,
            start_date_gte=start_date_gte,
            start_date_lte=start_date_lte,
            end_date_gte=end_date_gte,
            end_date_lte=end_date_lte,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /runs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# POST /api/orchestrator/dags/{dag_id}/runs
@router.post("/orchestrator/dags/{dag_id}/runs")
async def post_dag_run(dag_id: str, conf: Optional[dict] = None):
    try:
        run = await trigger_run(dag_id, conf)
        return {"message": "Run triggered", "run": run}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] POST /runs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# GET /api/orchestrator/dags/{dag_id}/runs/{run_id}/tasks
@router.get("/orchestrator/dags/{dag_id}/runs/{run_id}/tasks")
async def get_tasks(dag_id: str, run_id: str):
    try:
        task_instances = await get_task_instances(dag_id, run_id)
        return {"task_instances": task_instances}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /tasks error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# POST /api/orchestrator/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/action
@router.post("/orchestrator/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/action")
async def post_task_action(dag_id: str, run_id: str, task_id: str, request: TaskActionRequest):
    try:
        if request.action == "clear":
            result = await clear_task_instances(
                dag_id,
                run_id,
                task_id,
                include_upstream=request.include_upstream,
                include_downstream=request.include_downstream,
                include_future=request.include_future,
                include_past=request.include_past,
            )
            return {"message": "Task cleared", "result": result}

        state_map = {
            "mark_success": "success",
            "mark_failed": "failed",
            "mark_running": "running",
            "mark_queued": "queued",
        }
        new_state = state_map.get(request.action, request.new_state)
        if request.action == "set_state" and not new_state:
            raise HTTPException(status_code=400, detail="new_state is required for set_state")

        if new_state:
            result = await patch_task_instance_state(
                dag_id,
                run_id,
                task_id,
                new_state=new_state,
                map_index=request.map_index,
                note=request.note,
                include_upstream=request.include_upstream,
                include_downstream=request.include_downstream,
                include_future=request.include_future,
                include_past=request.include_past,
            )
            return {"message": f"Task updated to {new_state}", "result": result}

        raise HTTPException(status_code=400, detail="Invalid action")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] POST /tasks/action error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# GET /api/orchestrator/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/tries
@router.get("/orchestrator/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/tries")
async def get_task_tries_route(
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = Query(default=-1),
):
    try:
        return await get_task_instance_tries(dag_id, run_id, task_id, map_index=map_index)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /tries error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# GET /api/orchestrator/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs/{try_number}
@router.get("/orchestrator/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs/{try_number}")
async def get_task_logs_route(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    map_index: int = Query(default=-1),
    full_content: bool = Query(default=True),
    continuation_token: Optional[str] = Query(default=None),
):
    try:
        result = await get_task_logs(
            dag_id,
            run_id,
            task_id,
            try_number,
            map_index=map_index,
            full_content=full_content,
            continuation_token=continuation_token,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /logs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# GET /api/orchestrator/dags/{dag_id}/structure
@router.get("/orchestrator/dags/{dag_id}/structure")
async def get_structure(dag_id: str, run_id: Optional[str] = Query(default=None)):
    try:
        graph = await get_dag_structure(dag_id)

        # Overlay task instance state if run_id provided
        if run_id:
            task_instances = await get_task_instances(dag_id, run_id)
            state_map = {ti["task_id"]: ti for ti in task_instances}

            graph["nodes"] = [
                {
                    **node,
                    "state": state_map.get(node["id"], {}).get("state"),
                    "duration": state_map.get(node["id"], {}).get("duration"),
                    "mapIndex": state_map.get(node["id"], {}).get("map_index"),
                }
                for node in graph["nodes"]
            ]

        return graph
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] GET /structure error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# GET /api/orchestrator/health
@router.get("/orchestrator/health")
async def orchestrator_health():
    try:
        return await get_health()
    except HTTPException:
        raise
    except Exception as e:
        return {
            "scheduler": {"status": "unreachable", "heartbeat": None},
            "database": {"status": "unreachable"},
            "dag_processor": {"status": "unreachable", "heartbeat": None},
            "error": str(e),
        }


# POST /api/orchestrator/notebooks
@router.post("/orchestrator/notebooks")
async def get_notebook(request: NotebookRequest):
    try:
        # Step 1: Parse DAG files to find the application path
        task_map = build_task_notebook_map(request.dag_id)
        relative_path = task_map.get(request.task_id)

        # Fallback: try a global map if scoped lookup did not find task_id.
        if not relative_path and request.dag_id:
            task_map = build_task_notebook_map()
            relative_path = task_map.get(request.task_id)

        # Step 2: Fallback — scan notebooks directory
        if not relative_path:
            notebook_files = scan_notebooks(NOTEBOOKS_DIR)
            possible_names = [
                request.task_id,
                re.sub(r"^run_", "", request.task_id),
                re.sub(r"^run_\w+_", "", request.task_id),
            ]
            for name in possible_names:
                if name in notebook_files:
                    relative_path = notebook_files[name]
                    break
                if f"{name}.py" in notebook_files:
                    relative_path = notebook_files[f"{name}.py"]
                    break

        if not relative_path:
            available = ", ".join(task_map.keys()) if task_map else "none"
            raise HTTPException(
                status_code=404,
                detail=f"No notebook found for task: {request.task_id}. Available: {available}",
            )

        notebook_path = NOTEBOOKS_DIR / relative_path
        if not notebook_path.exists():
            raise HTTPException(
                status_code=404, detail=f"Notebook file not found: {relative_path}"
            )

        content = notebook_path.read_text(encoding="utf-8")

        return {
            "task_id": request.task_id,
            "filename": notebook_path.name,
            "path": f"notebooks/{relative_path}",
            "content": content,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Orchestrator] POST /notebooks error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
