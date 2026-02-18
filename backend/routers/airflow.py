"""
Airflow Router — CLI-based DAG management via Docker exec.
Ported from web-ui/src/app/api/airflow/route.ts
"""
import asyncio
import json
import logging
import time
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter()

# Track last auto-recovery to avoid restart loops (max once per 60s)
_last_recovery_time = 0
RECOVERY_COOLDOWN_S = 60


async def run_airflow_cli(command: str) -> str:
    """Execute Airflow CLI commands via Docker."""
    full_command = f"docker exec airflow_webserver airflow {command}"
    logger.info("[Airflow CLI] %s", full_command)

    proc = await asyncio.create_subprocess_shell(
        full_command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
    except asyncio.TimeoutError:
        proc.kill()
        raise HTTPException(status_code=500, detail="Airflow CLI timed out (15s)")

    stdout_str = stdout.decode().strip()
    stderr_str = stderr.decode().strip()

    if stderr_str and not stdout_str:
        logger.error("[Airflow CLI] stderr: %s", stderr_str)

    return stdout_str


class AirflowActionRequest(BaseModel):
    dag_id: str
    action: str  # "trigger" | "pause" | "unpause"


@router.get("/airflow")
async def airflow_get(action: str = Query(default="list_dags")):
    """List DAGs via Airflow CLI."""
    global _last_recovery_time

    try:
        if action == "list_dags":
            output = await run_airflow_cli("dags list -o json")
            try:
                cli_dags = json.loads(output)
            except json.JSONDecodeError:
                logger.error("[Airflow CLI] Failed to parse JSON: %s", output)
                raise HTTPException(status_code=500, detail="Failed to parse Airflow response")

            # Auto-Recovery: restart scheduler & dag-processor when 0 DAGs
            if len(cli_dags) == 0:
                now = time.time()
                if now - _last_recovery_time > RECOVERY_COOLDOWN_S:
                    _last_recovery_time = now
                    logger.warning(
                        "[Airflow Auto-Recovery] 0 DAGs detected — restarting scheduler & dag-processor..."
                    )
                    try:
                        proc = await asyncio.create_subprocess_shell(
                            "docker compose restart airflow-scheduler airflow-dag-processor",
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        await asyncio.wait_for(proc.communicate(), timeout=30)
                        # Wait for services to stabilize
                        await asyncio.sleep(8)
                        # Retry DAG list
                        retry_output = await run_airflow_cli("dags list -o json")
                        try:
                            cli_dags = json.loads(retry_output)
                            logger.info(
                                "[Airflow Auto-Recovery] ✅ Recovery succeeded — %d DAG(s) found",
                                len(cli_dags),
                            )
                        except json.JSONDecodeError:
                            logger.error("[Airflow Auto-Recovery] Retry parse failed: %s", retry_output)
                    except Exception as e:
                        logger.error("[Airflow Auto-Recovery] Recovery failed: %s", e)
                else:
                    logger.warning("[Airflow Auto-Recovery] Cooldown active — skipping restart")

            dags = [
                {
                    "dag_id": dag.get("dag_id"),
                    "owners": dag.get("owners", [])
                    if isinstance(dag.get("owners"), list)
                    else [dag.get("owners")],
                    "is_paused": dag.get("is_paused") in ("True", True),
                    "schedule_interval": None,
                    "fileloc": dag.get("fileloc"),
                }
                for dag in cli_dags
            ]

            return {"dags": dags, "total_entries": len(dags)}

        raise HTTPException(status_code=400, detail="Unknown action")

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Airflow CLI] GET Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/airflow")
async def airflow_post(request: AirflowActionRequest):
    """Trigger, pause, or unpause a DAG."""
    try:
        if request.action == "trigger":
            output = await run_airflow_cli(f"dags trigger {request.dag_id}")
        elif request.action == "pause":
            output = await run_airflow_cli(f"dags pause {request.dag_id}")
        elif request.action == "unpause":
            output = await run_airflow_cli(f"dags unpause {request.dag_id}")
        else:
            raise HTTPException(status_code=400, detail="Invalid action")

        return {"message": f"{request.action} completed", "output": output}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Airflow CLI] POST Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
