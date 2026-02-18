"""
Health Check Router — Proxy health checks to any service URL.
Ported from web-ui/src/app/api/health-check/route.ts
"""
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx

logger = logging.getLogger(__name__)

router = APIRouter()


class HealthCheckRequest(BaseModel):
    url: str


@router.post("/health-check")
async def health_check(request: HealthCheckRequest):
    """Check if a service is healthy by making a GET request to the given URL."""
    if not request.url:
        raise HTTPException(status_code=400, detail="URL is required")

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                request.url,
                headers={"Accept": "application/json, text/html, */*"},
            )
            return {
                "healthy": response.is_success,
                "status": response.status_code,
                "statusText": response.reason_phrase or "",
            }
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
