"""
Trino Router — Execute SQL queries against Trino.
Optimized with row-limit support and fast pagination.
"""
import logging
import re
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx

logger = logging.getLogger(__name__)

router = APIRouter()

TRINO_BASE_URL = "http://localhost:8083"
TRINO_URL = f"{TRINO_BASE_URL}/v1/statement"
TRINO_USER = "admin"

# Pre-compiled regex for checking if query already has a LIMIT clause
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)


class TrinoQueryRequest(BaseModel):
    query: str
    limit: Optional[int] = None


@router.post("/trino")
async def execute_trino_query(request: TrinoQueryRequest):
    """Execute a SQL query against Trino and return results."""
    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")

    # Trino API v1/statement does not support trailing semicolons
    if query.endswith(";"):
        query = query[:-1]

    # ─── Apply row limit if provided and query doesn't already have LIMIT ───
    # Only apply limit to SELECT-like queries, never to DDL/DML
    _DDL_PREFIXES = ('CREATE', 'DROP', 'ALTER', 'ANALYZE', 'GRANT', 'REVOKE', 'SET', 'RESET', 'USE', 'CALL', 'DEALLOCATE', 'PREPARE', 'EXECUTE', 'COMMENT', 'COMMIT', 'ROLLBACK', 'START', 'INSERT', 'UPDATE', 'DELETE', 'MERGE')
    is_ddl = query.upper().lstrip().startswith(_DDL_PREFIXES)
    has_limit = _LIMIT_PATTERN.search(query)
    effective_limit = None
    if request.limit and not has_limit and not is_ddl:
        query = f"{query} LIMIT {request.limit}"
        effective_limit = request.limit
    elif has_limit:
        # Extract existing limit value
        match = _LIMIT_PATTERN.search(query)
        if match:
            effective_limit = int(match.group().split()[-1])

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                TRINO_URL,
                content=query,
                headers={
                    "X-Trino-User": TRINO_USER,
                    "Content-Type": "text/plain",
                },
            )

            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Trino connection failed: {response.status_code} {response.text}",
                )

            result = response.json()

            # Pre-allocate list with estimated size
            all_data: list = []
            columns: list = []
            stats: dict = {}
            error = None
            page_count = 0

            while True:
                page_count += 1

                # Check for error
                if result.get("error"):
                    error = result["error"]
                    logger.error("[Trino] Error: %s", error)
                    break

                # Collect columns (first response)
                if result.get("columns") and not columns:
                    columns = result["columns"]

                # Collect data
                page_data = result.get("data")
                if page_data:
                    all_data.extend(page_data)

                # Early exit if we already have enough rows
                if effective_limit and len(all_data) >= effective_limit:
                    # Cancel remaining Trino query to free resources
                    cancel_uri = result.get("nextUri")
                    if cancel_uri:
                        try:
                            await client.delete(cancel_uri, headers={"X-Trino-User": TRINO_USER})
                        except Exception:
                            pass
                    all_data = all_data[:effective_limit]
                    break

                # Fetch next page
                next_uri = result.get("nextUri")
                if not next_uri:
                    break

                next_response = await client.get(
                    next_uri,
                    headers={"X-Trino-User": TRINO_USER},
                )

                if next_response.status_code != 200:
                    logger.error("[Trino] NextURI fetch failed: %s", next_response.status_code)
                    break

                result = next_response.json()

                # Safety break
                if page_count > 1000:
                    break

        logger.info("[Trino] Done. rows=%d cols=%d pages=%d", len(all_data), len(columns), page_count)

        if error:
            error_msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            raise HTTPException(status_code=400, detail=error_msg)

        # Format columns
        col_names = [col["name"] for col in columns] if columns else []

        # Format data: array of arrays → array of objects
        if col_names and all_data:
            formatted_data = [
                {col_names[i]: val for i, val in enumerate(row)}
                for row in all_data
            ]
        else:
            formatted_data = []

        return {
            "columns": col_names,
            "data": formatted_data,
            "stats": stats,
            "message": "Query executed successfully" if not col_names else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino API Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Trino REST API Wrappers (Full Native Access) ───


@router.get("/trino/info")
async def trino_cluster_info():
    """Get Trino cluster info: version, uptime, state, coordinator status."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{TRINO_BASE_URL}/v1/info",
                headers={"X-Trino-User": TRINO_USER},
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to reach Trino cluster")
            return resp.json()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino info error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trino/nodes")
async def trino_list_nodes():
    """List all Trino worker nodes with their status."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{TRINO_BASE_URL}/v1/node",
                headers={"X-Trino-User": TRINO_USER},
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to list Trino nodes")
            nodes = resp.json()

            # Also fetch failed nodes
            failed_resp = await client.get(
                f"{TRINO_BASE_URL}/v1/node/failed",
                headers={"X-Trino-User": TRINO_USER},
            )
            failed_nodes = failed_resp.json() if failed_resp.status_code == 200 else []

            return {
                "active": nodes,
                "failed": failed_nodes,
                "totalActive": len(nodes),
                "totalFailed": len(failed_nodes),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino nodes error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trino/queries")
async def trino_list_queries():
    """List all active and recently completed Trino queries."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{TRINO_BASE_URL}/v1/query",
                headers={"X-Trino-User": TRINO_USER},
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to list Trino queries")
            queries = resp.json()

            # Return summarized list (full details can be fetched per-query)
            summarized = []
            for q in queries:
                summarized.append({
                    "queryId": q.get("queryId"),
                    "state": q.get("state"),
                    "query": q.get("query", "")[:200],  # Truncate for list view
                    "queryType": q.get("queryType"),
                    "user": q.get("session", {}).get("user"),
                    "source": q.get("session", {}).get("source"),
                    "catalog": q.get("session", {}).get("catalog"),
                    "schema": q.get("session", {}).get("schema"),
                    "createTime": q.get("queryStats", {}).get("createTime"),
                    "endTime": q.get("queryStats", {}).get("endTime"),
                    "elapsedTime": q.get("queryStats", {}).get("elapsedTime"),
                    "cpuTime": q.get("queryStats", {}).get("totalCpuTime"),
                    "peakMemory": q.get("queryStats", {}).get("peakUserMemoryReservation"),
                    "completedSplits": q.get("queryStats", {}).get("completedDrivers"),
                    "totalSplits": q.get("queryStats", {}).get("totalDrivers"),
                    "processedRows": q.get("queryStats", {}).get("processedInputPositions"),
                    "processedBytes": q.get("queryStats", {}).get("processedInputDataSize"),
                })

            return {
                "queries": summarized,
                "total": len(summarized),
                "running": sum(1 for q in summarized if q["state"] in ("RUNNING", "PLANNING", "STARTING", "QUEUED")),
                "finished": sum(1 for q in summarized if q["state"] == "FINISHED"),
                "failed": sum(1 for q in summarized if q["state"] == "FAILED"),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino queries list error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trino/queries/{query_id}")
async def trino_query_detail(query_id: str):
    """Get detailed info about a specific Trino query including execution plan."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{TRINO_BASE_URL}/v1/query/{query_id}",
                headers={"X-Trino-User": TRINO_USER},
            )
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Query {query_id} not found")
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to fetch query detail")
            return resp.json()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino query detail error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/trino/queries/{query_id}")
async def trino_kill_query(query_id: str):
    """Kill a running Trino query."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(
                f"{TRINO_BASE_URL}/v1/query/{query_id}",
                headers={"X-Trino-User": TRINO_USER},
            )
            # Trino returns 204 No Content on successful kill
            if resp.status_code in (204, 200):
                return {"status": "killed", "queryId": query_id}
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Query {query_id} not found")
            else:
                raise HTTPException(status_code=502, detail=f"Failed to kill query: {resp.status_code}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino kill query error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
