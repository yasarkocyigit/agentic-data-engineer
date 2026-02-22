"""
Trino Router — Execute SQL queries against Trino.
Includes IDE-focused safety caps and rich query/session controls.
"""
import os
import logging
import re
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8083"))
TRINO_SCHEME = (os.getenv("TRINO_SCHEME", "http") or "http").strip().lower()
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")
TRINO_VERIFY_TLS = (os.getenv("TRINO_VERIFY_TLS", "false") or "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_REQUEST_TIMEOUT_SECONDS = float(os.getenv("TRINO_REQUEST_TIMEOUT_SECONDS", "120"))
_SHORT_TIMEOUT_SECONDS = float(os.getenv("TRINO_SHORT_TIMEOUT_SECONDS", "15"))
try:
    _MAX_RESULT_ROWS = max(1, int(os.getenv("TRINO_MAX_RESULT_ROWS", "10000")))
except ValueError:
    _MAX_RESULT_ROWS = 10000
try:
    _MAX_PAGE_COUNT = max(50, int(os.getenv("TRINO_MAX_PAGE_COUNT", "1000")))
except ValueError:
    _MAX_PAGE_COUNT = 1000

# Pre-compiled regex for checking if query already has a LIMIT clause
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_ROLE_PATTERN = re.compile(r"^[A-Za-z0-9_.:,\-=]{1,256}$")
_SESSION_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_READ_QUERY_PREFIX_PATTERN = re.compile(r"^\s*(SELECT|WITH|TABLE|VALUES)\b", re.IGNORECASE)
_WITH_DML_PATTERN = re.compile(r"^\s*WITH\b[\s\S]*\b(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE)


def _build_base_url(host: str, port: int) -> str:
    host = (host or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")
    scheme = TRINO_SCHEME if TRINO_SCHEME in ("http", "https") else "http"
    return f"{scheme}://{host}:{port}"


def _trino_base_urls() -> list[str]:
    primary = _build_base_url(TRINO_HOST, TRINO_PORT)
    fallback = [
        "http://localhost:8083",
        "http://host.docker.internal:8083",
        "http://trino:8080",
    ]
    return list(dict.fromkeys([primary, *fallback]))


def _create_trino_client(timeout_seconds: float) -> httpx.AsyncClient:
    auth = httpx.BasicAuth(TRINO_USER, TRINO_PASSWORD) if TRINO_PASSWORD else None
    return httpx.AsyncClient(timeout=timeout_seconds, verify=TRINO_VERIFY_TLS, auth=auth)


def _trino_headers(
    extra_headers: Optional[dict[str, str]] = None,
    *,
    include_content_type: bool = False,
) -> dict[str, str]:
    headers = {"X-Trino-User": TRINO_USER}
    if include_content_type:
        headers["Content-Type"] = "text/plain"
    if extra_headers:
        headers.update(extra_headers)
    return headers


async def _request_first_reachable(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    *,
    content: str | None = None,
    extra_headers: Optional[dict[str, str]] = None,
) -> tuple[httpx.Response, str]:
    last_exc: Exception | None = None
    for base in _trino_base_urls():
        url = f"{base}{path}"
        try:
            headers = _trino_headers(
                extra_headers,
                include_content_type=content is not None,
            )
            resp = await client.request(
                method,
                url,
                content=content,
                headers=headers,
            )
            return resp, base
        except Exception as exc:  # pragma: no cover - environment dependent
            last_exc = exc
            continue
    raise HTTPException(status_code=502, detail=f"Trino connection failed: {last_exc}")


class TrinoQueryRequest(BaseModel):
    query: str
    limit: Optional[int] = Field(default=None, ge=1)
    catalog: Optional[str] = None
    schema: Optional[str] = None
    query_tag: Optional[str] = None
    source: Optional[str] = None
    role: Optional[str] = None
    session_properties: Optional[dict[str, str]] = None
    client_tags: Optional[list[str]] = None


def _normalize_token(value: Optional[str], field_name: str) -> Optional[str]:
    if value is None:
        return None
    token = value.strip()
    if not token:
        raise HTTPException(status_code=400, detail=f"{field_name} cannot be empty")
    if not _TOKEN_PATTERN.match(token):
        raise HTTPException(status_code=400, detail=f"invalid {field_name}")
    return token


def _normalize_identifier(value: Optional[str], field_name: str) -> Optional[str]:
    if value is None:
        return None
    ident = value.strip()
    if not ident:
        raise HTTPException(status_code=400, detail=f"{field_name} cannot be empty")
    if not _IDENTIFIER_PATTERN.match(ident):
        raise HTTPException(status_code=400, detail=f"invalid {field_name}")
    return ident


def _normalize_role(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    role = value.strip()
    if not role:
        raise HTTPException(status_code=400, detail="role cannot be empty")
    if not _ROLE_PATTERN.match(role):
        raise HTTPException(status_code=400, detail="invalid role")
    return role


def _normalize_session_properties(props: Optional[dict[str, str]]) -> Optional[str]:
    if not props:
        return None
    pairs = []
    for key, value in props.items():
        prop_key = str(key).strip()
        prop_val = str(value).strip()
        if not prop_key or not _SESSION_KEY_PATTERN.match(prop_key):
            raise HTTPException(status_code=400, detail=f"invalid session property key: {key}")
        if not prop_val or not _TOKEN_PATTERN.match(prop_val):
            raise HTTPException(status_code=400, detail=f"invalid session property value for {prop_key}")
        pairs.append(f"{prop_key}={prop_val}")
    return ",".join(pairs)


def _normalize_client_tags(tags: Optional[list[str]]) -> Optional[str]:
    if not tags:
        return None
    normalized = []
    for item in tags:
        tag = _normalize_token(str(item), "client tag")
        if tag:
            normalized.append(tag)
    if not normalized:
        return None
    return ",".join(normalized)


async def _cancel_trino_query(client: httpx.AsyncClient, next_uri: Optional[str]) -> None:
    if not next_uri:
        return
    try:
        await client.delete(next_uri, headers=_trino_headers())
    except Exception:
        pass


def _is_limit_eligible_query(query: str) -> bool:
    """Return True when auto LIMIT can be safely appended."""
    stripped = query.lstrip()
    if not _READ_QUERY_PREFIX_PATTERN.match(stripped):
        return False
    # WITH can be used for data-changing statements; avoid mutating those.
    if stripped.upper().startswith("WITH") and _WITH_DML_PATTERN.search(stripped):
        return False
    return True


@router.post("/trino")
async def execute_trino_query(request: TrinoQueryRequest):
    """Execute a SQL query against Trino and return results."""
    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")

    headers: dict[str, str] = {}
    catalog = _normalize_identifier(request.catalog, "catalog")
    if catalog:
        headers["X-Trino-Catalog"] = catalog
    schema = _normalize_identifier(request.schema, "schema")
    if schema:
        headers["X-Trino-Schema"] = schema

    query_tag = _normalize_token(request.query_tag, "query_tag")
    source = _normalize_token(request.source, "source")
    query_source = f"openclaw:{query_tag}" if query_tag else source
    if query_source:
        headers["X-Trino-Source"] = query_source

    role = _normalize_role(request.role)
    if role:
        headers["X-Trino-Role"] = role

    session_header = _normalize_session_properties(request.session_properties)
    if session_header:
        headers["X-Trino-Session"] = session_header

    client_tags = _normalize_client_tags(request.client_tags)
    if client_tags:
        headers["X-Trino-Client-Tags"] = client_tags

    # Trino API v1/statement does not support trailing semicolons
    if query.endswith(";"):
        query = query[:-1]

    # ─── Apply row limit for safe read-only queries when needed ───
    has_limit = _LIMIT_PATTERN.search(query)
    effective_limit = None
    is_limit_eligible = _is_limit_eligible_query(query)
    if request.limit and not has_limit and is_limit_eligible:
        query = f"{query} LIMIT {request.limit}"
        effective_limit = request.limit
    elif has_limit:
        # Extract existing limit value
        match = _LIMIT_PATTERN.search(query)
        if match:
            try:
                effective_limit = int(match.group().split()[-1])
            except ValueError:
                effective_limit = None

    if effective_limit is not None and effective_limit <= _MAX_RESULT_ROWS:
        row_cap = effective_limit
        cap_reason = "query_limit"
    else:
        row_cap = _MAX_RESULT_ROWS
        cap_reason = "server_cap"

    try:
        async with _create_trino_client(_REQUEST_TIMEOUT_SECONDS) as client:
            response, _ = await _request_first_reachable(
                client,
                "POST",
                "/v1/statement",
                content=query,
                extra_headers=headers,
            )

            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Trino connection failed: {response.status_code} {response.text}",
                )

            result = response.json()
            query_id = result.get("id")

            # Pre-allocate list with estimated size
            all_data: list = []
            columns: list = []
            stats: dict = {}
            warnings: list[str] = []
            update_type = None
            update_count = None
            error = None
            page_count = 0
            was_truncated = False

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

                # Collect warning messages for output panel
                for warning in (result.get("warnings") or []):
                    if isinstance(warning, dict):
                        message = warning.get("message") or warning.get("warningCode")
                    else:
                        message = str(warning)
                    if message:
                        text = str(message).strip()
                        if text and text not in warnings:
                            warnings.append(text)

                if result.get("updateType"):
                    update_type = result.get("updateType")
                if result.get("updateCount") is not None:
                    update_count = result.get("updateCount")

                # Collect data
                page_data = result.get("data")
                if page_data:
                    all_data.extend(page_data)

                # Keep latest execution stats for UI status/metrics.
                if result.get("stats"):
                    stats = result["stats"]

                # Early exit if we already have enough rows for either user limit or server cap
                if len(all_data) >= row_cap:
                    has_more = len(all_data) > row_cap or bool(result.get("nextUri"))
                    if len(all_data) > row_cap:
                        all_data = all_data[:row_cap]
                    if has_more:
                        await _cancel_trino_query(client, result.get("nextUri"))
                    if cap_reason == "server_cap" and has_more:
                        was_truncated = True
                    break

                # Fetch next page
                next_uri = result.get("nextUri")
                if not next_uri:
                    break

                next_response = await client.get(
                    next_uri,
                    headers=_trino_headers(),
                )

                if next_response.status_code != 200:
                    logger.error("[Trino] NextURI fetch failed: %s", next_response.status_code)
                    break

                result = next_response.json()

                # Safety break
                if page_count > _MAX_PAGE_COUNT:
                    warnings.append("Result paging stopped at safety page limit.")
                    await _cancel_trino_query(client, result.get("nextUri"))
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

        completion_message = None
        if not col_names:
            if update_type:
                if isinstance(update_count, int):
                    completion_message = f"{update_type} {update_count}"
                else:
                    completion_message = f"{update_type} completed"
            else:
                completion_message = "Query executed successfully"

        return {
            "columns": col_names,
            "data": formatted_data,
            "warnings": warnings,
            "stats": {
                **stats,
                "queryId": query_id,
                "queryTag": query_tag,
                "querySource": query_source,
                "appliedLimit": effective_limit,
                "resultCap": row_cap,
                "truncated": was_truncated,
                "capReason": cap_reason,
                "updateType": update_type,
                "updateCount": update_count,
            },
            "message": completion_message,
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
        async with _create_trino_client(_SHORT_TIMEOUT_SECONDS) as client:
            resp, _ = await _request_first_reachable(client, "GET", "/v1/info")
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
        async with _create_trino_client(_SHORT_TIMEOUT_SECONDS) as client:
            resp, base = await _request_first_reachable(client, "GET", "/v1/node")
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to list Trino nodes")
            nodes = resp.json()

            # Also fetch failed nodes
            failed_resp = await client.get(
                f"{base}/v1/node/failed",
                headers=_trino_headers(),
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
        async with _create_trino_client(_SHORT_TIMEOUT_SECONDS) as client:
            resp, _ = await _request_first_reachable(client, "GET", "/v1/query")
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to list Trino queries")
            queries = resp.json()

            # Return summarized list (full details can be fetched per-query)
            summarized = []
            for q in queries:
                full_query = q.get("query", "") or ""
                source = q.get("session", {}).get("source")
                query_tag = None
                if isinstance(source, str) and source.startswith("openclaw:"):
                    query_tag = source.split(":", 1)[1].split("|", 1)[0]
                summarized.append({
                    "queryId": q.get("queryId"),
                    "state": q.get("state"),
                    "query": full_query,
                    "queryShort": (full_query[:200] + "...") if len(full_query) > 200 else full_query,
                    "queryType": q.get("queryType"),
                    "user": q.get("session", {}).get("user"),
                    "source": source,
                    "queryTag": query_tag,
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
        async with _create_trino_client(_SHORT_TIMEOUT_SECONDS) as client:
            resp, _ = await _request_first_reachable(client, "GET", f"/v1/query/{query_id}")
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
        async with _create_trino_client(_SHORT_TIMEOUT_SECONDS) as client:
            resp, _ = await _request_first_reachable(client, "DELETE", f"/v1/query/{query_id}")
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
