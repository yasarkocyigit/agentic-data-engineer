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
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

router = APIRouter()

AIRFLOW_BASE = os.getenv("AIRFLOW_API_URL", "http://localhost:8081")
API_PREFIX = "/api/v2"
SA_USERNAME = os.getenv("AIRFLOW_SA_USERNAME", "admin")
SA_PASSWORD = os.getenv("AIRFLOW_SA_PASSWORD", "admin")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
DAGS_DIR = PROJECT_ROOT / "dags"
DOCKER_NOTEBOOK_PREFIX = "/opt/airflow/notebooks/"

# ─── JWT Token Manager ───

_cached_token: Optional[str] = None
_token_expires_at: float = 0


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


async def airflow_fetch(path: str, method: str = "GET", body: dict = None) -> dict:
    """Authenticated request to Airflow REST API."""
    token = await get_airflow_token()
    url = f"{AIRFLOW_BASE}{API_PREFIX}{path}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        kwargs = {
            "headers": {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        }
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

    # Fetch last run for each DAG concurrently
    async def get_dag_with_run(dag):
        last_run = None
        try:
            runs = await airflow_fetch(
                f"/dags/{dag['dag_id']}/dagRuns?order_by=-start_date&limit=1"
            )
            if runs.get("dag_runs") and len(runs["dag_runs"]) > 0:
                last_run = transform_run(runs["dag_runs"][0])
        except Exception:
            pass
        return transform_dag(dag, last_run)

    dags_with_runs = await asyncio.gather(
        *[get_dag_with_run(dag) for dag in data.get("dags", [])]
    )

    return {"dags": dags_with_runs, "total": data.get("total_entries", 0)}


async def get_dag(dag_id: str):
    dag = await airflow_fetch(f"/dags/{dag_id}")
    last_run = None
    try:
        runs = await airflow_fetch(f"/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1")
        if runs.get("dag_runs") and len(runs["dag_runs"]) > 0:
            last_run = transform_run(runs["dag_runs"][0])
    except Exception:
        pass
    return transform_dag(dag, last_run)


async def list_runs(dag_id: str, limit=25, offset=0, state=None):
    params = f"order_by=-start_date&limit={limit}&offset={offset}"
    if state:
        params += f"&state={state}"
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
    return transform_run(result)


async def cancel_run(dag_id: str, run_id: str):
    result = await airflow_fetch(
        f"/dags/{dag_id}/dagRuns/{run_id}",
        method="PATCH",
        body={"state": "failed"},
    )
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


# ─── Notebook Resolution ───


def build_task_notebook_map() -> dict:
    """Parse DAG files to build task_id → notebook path mapping."""
    mapping = {}
    if not DAGS_DIR.exists():
        return mapping

    for dag_file in DAGS_DIR.glob("*.py"):
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
):
    try:
        return await list_runs(dag_id, limit=limit, offset=offset, state=state)
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
