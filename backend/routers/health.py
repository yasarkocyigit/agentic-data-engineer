"""
Health Check Router — Proxy health checks to any service URL.
Ported from web-ui/src/app/api/health-check/route.ts
"""
import asyncio
import json
import logging
import re
import subprocess
from urllib.parse import urlparse, urlunparse

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx

logger = logging.getLogger(__name__)

router = APIRouter()


class HealthCheckRequest(BaseModel):
    url: str


_CONTAINER_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def _build_url_candidates(url: str) -> list[str]:
    candidates = [url]
    parsed = urlparse(url)
    if parsed.hostname in {"localhost", "127.0.0.1"}:
        alt_netloc = parsed.netloc.replace(parsed.hostname, "host.docker.internal", 1)
        alt_url = urlunparse(parsed._replace(netloc=alt_netloc))
        if alt_url not in candidates:
            candidates.append(alt_url)
    return candidates


async def _check_docker_container(container_name: str) -> dict:
    if not _CONTAINER_NAME_PATTERN.match(container_name):
        return {"healthy": False, "status": 400, "statusText": "Invalid container name"}

    proc = await asyncio.create_subprocess_exec(
        "docker",
        "inspect",
        "--format",
        "{{json .State}}",
        container_name,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        error = stderr.decode().strip() or stdout.decode().strip() or "Container not found"
        return {"healthy": False, "status": 404, "statusText": error}

    try:
        state = json.loads(stdout.decode().strip() or "{}")
    except Exception:
        return {"healthy": False, "status": 500, "statusText": "Failed to parse docker state"}

    running = bool(state.get("Running", False))
    health = state.get("Health") or {}
    health_status = str(health.get("Status", "")).strip().lower()

    if not running:
        status_text = str(state.get("Status") or "stopped")
        return {"healthy": False, "status": 503, "statusText": status_text}

    if health_status and health_status != "healthy":
        return {"healthy": False, "status": 503, "statusText": f"running ({health_status})"}

    return {"healthy": True, "status": 200, "statusText": "running"}


@router.post("/health-check")
async def health_check(request: HealthCheckRequest):
    """Check if a service is healthy by making a GET request to the given URL."""
    if not request.url:
        raise HTTPException(status_code=400, detail="URL is required")

    if request.url.startswith("docker://"):
        container_name = request.url[len("docker://"):].strip()
        return await _check_docker_container(container_name)

    last_error = "Connection refused"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for target_url in _build_url_candidates(request.url):
                try:
                    response = await client.get(
                        target_url,
                        headers={"Accept": "application/json, text/html, */*"},
                    )
                    return {
                        "healthy": response.is_success,
                        "status": response.status_code,
                        "statusText": response.reason_phrase or "",
                    }
                except httpx.TimeoutException:
                    last_error = "Timeout (5s)"
                except Exception as exc:
                    last_error = str(exc) or "Connection refused"
            return {"healthy": False, "status": 0, "statusText": last_error}
    except httpx.TimeoutException:
        return {
            "healthy": False,
            "status": 0,
            "statusText": "Timeout (5s)",
        }
    except Exception as e:
        return {
            "healthy": False,
            "status": 0,
            "statusText": str(e),
        }
