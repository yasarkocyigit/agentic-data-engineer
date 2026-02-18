"""
Lineage Router — Proxy to Marquez API for data lineage.
Ported from web-ui/src/app/api/lineage/route.ts
"""
import logging
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

router = APIRouter()

MARQUEZ_URL = "http://localhost:5002"


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

        if action == "namespaces":
            endpoint = "/api/v1/namespaces"

        elif action == "jobs":
            if not ns:
                raise HTTPException(status_code=400, detail="ns (namespace) is required")
            endpoint = f"/api/v1/namespaces/{ns}/jobs?limit={limit}"

        elif action == "datasets":
            if not ns:
                raise HTTPException(status_code=400, detail="ns (namespace) is required")
            endpoint = f"/api/v1/namespaces/{ns}/datasets?limit={limit}"

        elif action == "lineage":
            if not nodeId:
                raise HTTPException(status_code=400, detail="nodeId is required")
            endpoint = f"/api/v1/lineage?nodeId={nodeId}&depth={depth}"

        elif action == "dataset":
            if not ns or not name:
                raise HTTPException(status_code=400, detail="ns and name required")
            endpoint = f"/api/v1/namespaces/{ns}/datasets/{name}"

        elif action == "job":
            if not ns or not name:
                raise HTTPException(status_code=400, detail="ns and name required")
            endpoint = f"/api/v1/namespaces/{ns}/jobs/{name}"

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid action. Use: namespaces, jobs, datasets, lineage, dataset, job",
            )

        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(
                f"{MARQUEZ_URL}{endpoint}",
                headers={"Accept": "application/json"},
            )

            if res.status_code != 200:
                raise HTTPException(
                    status_code=res.status_code,
                    detail=f"Marquez API error: {res.status_code} {res.text}",
                )

            return res.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Lineage API Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
