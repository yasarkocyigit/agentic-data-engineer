"""
Lineage Router — Proxy to Marquez API for data lineage.
Ported from web-ui/src/app/api/lineage/route.ts
"""

import logging
import os
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter()

MARQUEZ_URL = (os.getenv("MARQUEZ_URL") or "http://marquez:5000").rstrip("/")


def _normalize_positive_int(raw: str, default: int, field_name: str) -> int:
    value = (raw or "").strip()
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be an integer") from exc
    if parsed < 1:
        raise HTTPException(status_code=400, detail=f"{field_name} must be >= 1")
    return parsed


@router.get("/lineage")
async def lineage_handler(
    action: Optional[str] = Query(None),
    ns: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    nodeId: Optional[str] = Query(None),
    depth: str = Query(default="5"),
    limit: str = Query(default="100"),
):
    """Proxy requests to Marquez API for lineage data."""
    try:
        endpoint = ""
        action_name = (action or "").strip().lower()
        safe_limit = _normalize_positive_int(limit, 100, "limit")
        safe_depth = _normalize_positive_int(depth, 5, "depth")

        if action_name == "namespaces":
            endpoint = "/api/v1/namespaces"

        elif action_name == "jobs":
            if not ns:
                raise HTTPException(status_code=400, detail="ns (namespace) is required")
            endpoint = f"/api/v1/namespaces/{quote(ns, safe='')}/jobs?limit={safe_limit}"

        elif action_name == "datasets":
            if not ns:
                raise HTTPException(status_code=400, detail="ns (namespace) is required")
            endpoint = f"/api/v1/namespaces/{quote(ns, safe='')}/datasets?limit={safe_limit}"

        elif action_name == "lineage":
            if not nodeId:
                raise HTTPException(status_code=400, detail="nodeId is required")
            endpoint = f"/api/v1/lineage?nodeId={quote(nodeId, safe='')}&depth={safe_depth}"

        elif action_name == "dataset":
            if not ns or not name:
                raise HTTPException(status_code=400, detail="ns and name required")
            endpoint = f"/api/v1/namespaces/{quote(ns, safe='')}/datasets/{quote(name, safe='')}"

        elif action_name == "job":
            if not ns or not name:
                raise HTTPException(status_code=400, detail="ns and name required")
            endpoint = f"/api/v1/namespaces/{quote(ns, safe='')}/jobs/{quote(name, safe='')}"

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid action. Use: namespaces, jobs, datasets, lineage, dataset, job",
            )

        async with httpx.AsyncClient(timeout=10.0) as client:
            target_url = f"{MARQUEZ_URL}{endpoint}"
            try:
                res = await client.get(target_url, headers={"Accept": "application/json"})
            except httpx.RequestError as exc:
                raise HTTPException(
                    status_code=502,
                    detail=f"Cannot reach Marquez at {MARQUEZ_URL}: {exc}",
                ) from exc

            if res.status_code != 200:
                body = res.text[:1000]
                raise HTTPException(
                    status_code=res.status_code,
                    detail=f"Marquez API error ({res.status_code}) for {endpoint}: {body}",
                )

            return res.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Lineage API Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
