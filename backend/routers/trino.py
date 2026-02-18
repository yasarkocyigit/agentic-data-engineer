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

TRINO_URL = "http://localhost:8083/v1/statement"
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
    has_limit = _LIMIT_PATTERN.search(query)
    effective_limit = None
    if request.limit and not has_limit:
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
        col_names = [col["name"] for col in columns]

        # Format data: array of arrays → array of objects
        # Use list comprehension for speed instead of loop + dict assignment
        formatted_data = [
            {col_names[i]: val for i, val in enumerate(row)}
            for row in all_data
        ]

        return {
            "columns": col_names,
            "data": formatted_data,
            "stats": stats,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trino API Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
