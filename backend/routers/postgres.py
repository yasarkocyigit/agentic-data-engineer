"""
PostgreSQL Router — Execute queries and explore schema.
Ported from web-ui/src/app/api/postgres/route.ts
Uses psycopg v3 (modern PostgreSQL driver for Python).
"""
import os
import logging
import re
import time
import threading
from contextlib import contextmanager
from typing import Optional

import psycopg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

router = APIRouter()

# ─── Connection Settings ───
PG_HOST = os.getenv("POSTGRES_HOST") or os.getenv("PG_HOST") or "localhost"
PG_PORT = int(os.getenv("POSTGRES_PORT") or os.getenv("PG_PORT") or "5433")
PG_USER = os.getenv("POSTGRES_USER") or os.getenv("PG_USER") or "postgres"
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD") or os.getenv("PG_PASSWORD")
PG_DEFAULT_DB = os.getenv("POSTGRES_DB") or os.getenv("PG_DATABASE") or "controldb"

try:
    _MAX_RESULT_ROWS = max(1, int(os.getenv("POSTGRES_MAX_RESULT_ROWS", "10000")))
except ValueError:
    _MAX_RESULT_ROWS = 10000

_DB_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)
_SELECT_LIKE_PATTERN = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")

_SESSION_IDLE_TTL_SECONDS = int(os.getenv("POSTGRES_SESSION_IDLE_TTL_SECONDS", "1800"))
_PG_SESSIONS_LOCK = threading.Lock()
_PG_SESSIONS: dict[str, dict[str, object]] = {}


def _normalize_database_name(database: Optional[str]) -> str:
    dbname = (database or PG_DEFAULT_DB).strip()
    if not dbname:
        raise HTTPException(status_code=400, detail="database cannot be empty")
    if not _DB_NAME_PATTERN.match(dbname):
        raise HTTPException(status_code=400, detail="invalid database name")
    return dbname


def _connection_host_candidates() -> list[str]:
    host = (PG_HOST or "").strip() or "localhost"
    candidates = [host]
    if host == "localhost":
        candidates.append("host.docker.internal")
    return list(dict.fromkeys(candidates))


def _normalize_token(value: Optional[str], field_name: str) -> Optional[str]:
    if value is None:
        return None
    token = value.strip()
    if not token:
        raise HTTPException(status_code=400, detail=f"{field_name} cannot be empty")
    if not _TOKEN_PATTERN.match(token):
        raise HTTPException(status_code=400, detail=f"invalid {field_name}")
    return token


def _open_connection(database: Optional[str] = None):
    dbname = _normalize_database_name(database)
    last_exc: Exception | None = None
    conn = None
    for host in _connection_host_candidates():
        try:
            conn = psycopg.connect(
                host=host,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=dbname,
                connect_timeout=5,
                row_factory=dict_row,
                autocommit=True,
            )
            break
        except Exception as exc:  # pragma: no cover - exercised in runtime environments
            last_exc = exc

    if conn is None:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Failed to establish PostgreSQL connection")
    return conn


def _cleanup_idle_sessions_locked(now: float) -> None:
    stale_ids = []
    for session_id, state in _PG_SESSIONS.items():
        last_used = float(state.get("last_used", 0.0))
        conn = state.get("conn")
        if conn is None:
            stale_ids.append(session_id)
            continue
        if getattr(conn, "closed", False) or now - last_used > _SESSION_IDLE_TTL_SECONDS:
            try:
                conn.close()
            except Exception:
                pass
            stale_ids.append(session_id)

    for session_id in stale_ids:
        _PG_SESSIONS.pop(session_id, None)


def _get_or_create_session_state(session_id: str, database: Optional[str]) -> dict[str, object]:
    dbname = _normalize_database_name(database)
    now = time.time()
    with _PG_SESSIONS_LOCK:
        _cleanup_idle_sessions_locked(now)
        state = _PG_SESSIONS.get(session_id)
        if state:
            conn = state.get("conn")
            state_db = state.get("database")
            if conn is None or getattr(conn, "closed", False) or state_db != dbname:
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                _PG_SESSIONS.pop(session_id, None)
                state = None

        if state is None:
            conn = _open_connection(dbname)
            state = {
                "conn": conn,
                "database": dbname,
                "last_used": now,
                "lock": threading.Lock(),
            }
            _PG_SESSIONS[session_id] = state
        else:
            state["last_used"] = now
        return state


def _close_session(session_id: str) -> bool:
    with _PG_SESSIONS_LOCK:
        state = _PG_SESSIONS.pop(session_id, None)
    if not state:
        return False
    conn = state.get("conn")
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    return True


def _mark_session_used(session_id: str) -> None:
    with _PG_SESSIONS_LOCK:
        if session_id in _PG_SESSIONS:
            _PG_SESSIONS[session_id]["last_used"] = time.time()


def _is_connection_in_transaction(conn) -> bool:
    status = conn.info.transaction_status
    return status in (TransactionStatus.INTRANS, TransactionStatus.INERROR, TransactionStatus.ACTIVE)


def _apply_row_limit(query: str, limit: Optional[int]) -> tuple[str, Optional[int]]:
    sql_query = query.strip()
    if sql_query.endswith(";"):
        sql_query = sql_query[:-1]

    if not limit:
        return sql_query, None

    if _LIMIT_PATTERN.search(sql_query):
        return sql_query, limit

    if _SELECT_LIKE_PATTERN.match(sql_query):
        return f"{sql_query} LIMIT {limit}", limit

    return sql_query, None


@contextmanager
def get_connection(database: Optional[str] = None):
    """Get a PostgreSQL connection using psycopg v3."""
    conn = _open_connection(database)

    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def acquire_request_connection(database: Optional[str], session_id: Optional[str]):
    normalized_session_id = _normalize_token(session_id, "session_id")
    if normalized_session_id:
        state = _get_or_create_session_state(normalized_session_id, database)
        session_lock = state["lock"]
        session_lock.acquire()
        try:
            conn = state["conn"]
            if conn is None or getattr(conn, "closed", False):
                raise HTTPException(status_code=500, detail="session connection is not available")
            yield conn, normalized_session_id
        finally:
            _mark_session_used(normalized_session_id)
            session_lock.release()
        return

    with get_connection(database) as conn:
        yield conn, None


class PostgresRequest(BaseModel):
    action: str  # "query" or "explore"
    query: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table: Optional[str] = None
    limit: Optional[int] = Field(default=None, ge=1)
    type: Optional[str] = None  # for explore: databases, schemas, tables, columns
    session_id: Optional[str] = None
    query_tag: Optional[str] = None
    routine_oid: Optional[int] = Field(default=None, ge=1)
    close_session: bool = False

    def __init__(self, **data):
        # Map "schema" key from frontend to "schema_name"
        if "schema" in data and "schema_name" not in data:
            data["schema_name"] = data.pop("schema")
        super().__init__(**data)


@router.post("/postgres")
async def postgres_handler(request: PostgresRequest):
    """Execute a PostgreSQL query or explore database metadata."""
    try:
        query_tag = _normalize_token(request.query_tag, "query_tag")
        with acquire_request_connection(request.database, request.session_id) as (conn, active_session_id):
            cursor = conn.cursor()

            if request.action == "query":
                # ─── Execute SQL Query ───
                if not request.query:
                    raise HTTPException(status_code=400, detail="Query is required")

                sql_query, applied_limit = _apply_row_limit(request.query, request.limit)
                if query_tag:
                    # SET does not support bind params reliably in extended protocol.
                    # Use set_config so query_tag can still be passed as a parameter.
                    cursor.execute(
                        "SELECT set_config('application_name', %s, false)",
                        (f"openclaw:{query_tag}",),
                    )
                start_time = time.time()
                cursor.execute(sql_query)
                end_time = time.time()

                columns = [desc.name for desc in cursor.description] if cursor.description else []
                was_truncated = False
                max_rows = _MAX_RESULT_ROWS if columns else None
                if columns:
                    rows = cursor.fetchmany(_MAX_RESULT_ROWS + 1)
                    if len(rows) > _MAX_RESULT_ROWS:
                        was_truncated = True
                        rows = rows[:_MAX_RESULT_ROWS]
                    data = [dict(row) for row in rows]
                    row_count = len(data)
                else:
                    data = []
                    row_count = cursor.rowcount

                notices: list[str] = []
                raw_notices = getattr(conn, "notices", None)
                if raw_notices:
                    try:
                        for notice in list(raw_notices)[-20:]:
                            text = str(notice).strip()
                            if text:
                                notices.append(text)
                    except Exception:
                        notices = []
                    try:
                        raw_notices.clear()
                    except Exception:
                        try:
                            del raw_notices[:]
                        except Exception:
                            pass

                payload = {
                    "columns": columns,
                    "data": data,
                    "notices": notices,
                    "stats": {
                        "rowCount": row_count,
                        "duration": int((end_time - start_time) * 1000),
                        "command": cursor.statusmessage,
                        "appliedLimit": applied_limit,
                        "backendPid": conn.info.backend_pid,
                        "inTransaction": _is_connection_in_transaction(conn),
                        "sessionId": active_session_id,
                        "queryTag": query_tag,
                        "truncated": was_truncated,
                        "maxRows": max_rows,
                    },
                }

                if request.close_session and active_session_id:
                    _close_session(active_session_id)
                return payload

            elif request.action == "explore":
                # ─── Schema Exploration ───
                explore_type = request.type

                if explore_type == "databases":
                    cursor.execute(
                        "SELECT datname AS name FROM pg_database WHERE datistemplate = false ORDER BY datname"
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "schemas":
                    cursor.execute(
                        "SELECT schema_name AS name FROM information_schema.schemata "
                        "WHERE schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema') "
                        "ORDER BY schema_name"
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "tables":
                    if not request.schema_name:
                        raise HTTPException(status_code=400, detail="schema is required")
                    cursor.execute(
                        "SELECT table_name AS name, 'table' AS object_type "
                        "FROM information_schema.tables "
                        "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                        "ORDER BY table_name",
                        (request.schema_name,),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "views":
                    if not request.schema_name:
                        raise HTTPException(status_code=400, detail="schema is required")
                    cursor.execute(
                        "SELECT table_name AS name, 'view' AS object_type "
                        "FROM information_schema.views "
                        "WHERE table_schema = %s "
                        "UNION ALL "
                        "SELECT matviewname AS name, 'materialized_view' AS object_type "
                        "FROM pg_matviews WHERE schemaname = %s "
                        "ORDER BY name",
                        (request.schema_name, request.schema_name),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "functions":
                    if not request.schema_name:
                        raise HTTPException(status_code=400, detail="schema is required")
                    cursor.execute(
                        "SELECT p.oid::bigint AS oid, "
                        "p.proname AS name, "
                        "p.proname || '(' || COALESCE(pg_get_function_identity_arguments(p.oid), '') || ')' AS display_name, "
                        "COALESCE(pg_get_function_identity_arguments(p.oid), '') AS identity_arguments, "
                        "CASE p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END AS object_type "
                        "FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = %s "
                        "ORDER BY p.proname, p.prokind, pg_get_function_identity_arguments(p.oid)",
                        (request.schema_name,),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "sequences":
                    if not request.schema_name:
                        raise HTTPException(status_code=400, detail="schema is required")
                    cursor.execute(
                        "SELECT sequence_name AS name, 'sequence' AS object_type "
                        "FROM information_schema.sequences "
                        "WHERE sequence_schema = %s "
                        "ORDER BY sequence_name",
                        (request.schema_name,),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "types":
                    if not request.schema_name:
                        raise HTTPException(status_code=400, detail="schema is required")
                    cursor.execute(
                        "SELECT t.typname AS name, 'type' AS object_type "
                        "FROM pg_type t "
                        "JOIN pg_namespace n ON n.oid = t.typnamespace "
                        "WHERE n.nspname = %s "
                        "AND t.typtype IN ('e', 'c', 'd', 'r') "
                        "AND left(t.typname, 1) <> '_' "
                        "ORDER BY t.typname",
                        (request.schema_name,),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "extensions":
                    cursor.execute(
                        "SELECT extname AS name, 'extension' AS object_type "
                        "FROM pg_extension "
                        "ORDER BY extname"
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "columns":
                    if not request.schema_name or not request.table:
                        raise HTTPException(status_code=400, detail="schema and table are required")
                    cursor.execute(
                        "SELECT column_name AS name, data_type, is_nullable, column_default "
                        "FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ordinal_position",
                        (request.schema_name, request.table),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "indexes":
                    if not request.schema_name or not request.table:
                        raise HTTPException(status_code=400, detail="schema and table are required")
                    cursor.execute(
                        "SELECT indexname, indexdef "
                        "FROM pg_indexes "
                        "WHERE schemaname = %s AND tablename = %s "
                        "ORDER BY indexname",
                        (request.schema_name, request.table),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "constraints":
                    if not request.schema_name or not request.table:
                        raise HTTPException(status_code=400, detail="schema and table are required")
                    cursor.execute(
                        "SELECT tc.constraint_name, tc.constraint_type, "
                        "kcu.column_name, ccu.table_schema AS foreign_table_schema, "
                        "ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name "
                        "FROM information_schema.table_constraints tc "
                        "LEFT JOIN information_schema.key_column_usage kcu "
                        "ON tc.constraint_name = kcu.constraint_name "
                        "AND tc.table_schema = kcu.table_schema "
                        "AND tc.table_name = kcu.table_name "
                        "LEFT JOIN information_schema.constraint_column_usage ccu "
                        "ON tc.constraint_name = ccu.constraint_name "
                        "AND tc.table_schema = ccu.table_schema "
                        "WHERE tc.table_schema = %s AND tc.table_name = %s "
                        "ORDER BY tc.constraint_name, kcu.ordinal_position",
                        (request.schema_name, request.table),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "triggers":
                    if not request.schema_name or not request.table:
                        raise HTTPException(status_code=400, detail="schema and table are required")
                    cursor.execute(
                        "SELECT trigger_name, event_manipulation, action_timing, action_statement "
                        "FROM information_schema.triggers "
                        "WHERE event_object_schema = %s AND event_object_table = %s "
                        "ORDER BY trigger_name",
                        (request.schema_name, request.table),
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "ddl":
                    if not request.schema_name or not request.table:
                        raise HTTPException(status_code=400, detail="schema and table are required")

                    # Function/procedure DDL (supports overloaded routines via routine_oid)
                    if request.routine_oid:
                        cursor.execute(
                            "SELECT pg_get_functiondef(p.oid) AS definition "
                            "FROM pg_proc p "
                            "WHERE p.oid = %s "
                            "LIMIT 1",
                            (request.routine_oid,),
                        )
                    else:
                        cursor.execute(
                            "SELECT pg_get_functiondef(p.oid) AS definition "
                            "FROM pg_proc p "
                            "JOIN pg_namespace n ON n.oid = p.pronamespace "
                            "WHERE n.nspname = %s AND p.proname = %s "
                            "ORDER BY p.oid "
                            "LIMIT 1",
                            (request.schema_name, request.table),
                        )
                    routine_ddl = cursor.fetchone()
                    if routine_ddl and routine_ddl.get("definition"):
                        return {"data": {"definition": routine_ddl["definition"]}}

                    # View/materialized view DDL
                    cursor.execute(
                        "SELECT c.relkind, pg_get_viewdef(c.oid, true) AS definition "
                        "FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('v', 'm') "
                        "LIMIT 1",
                        (request.schema_name, request.table),
                    )
                    view_ddl = cursor.fetchone()
                    if view_ddl and view_ddl.get("definition"):
                        kind = "MATERIALIZED VIEW" if view_ddl["relkind"] == "m" else "VIEW"
                        return {
                            "data": {
                                "definition": (
                                    f"CREATE {kind} {request.schema_name}.{request.table} AS\n"
                                    f"{view_ddl['definition']};"
                                )
                            }
                        }

                    # Table-like DDL (best-effort)
                    cursor.execute(
                        "SELECT column_name, data_type, character_maximum_length, "
                        "numeric_precision, numeric_scale, is_nullable, column_default "
                        "FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ordinal_position",
                        (request.schema_name, request.table),
                    )
                    cols = cursor.fetchall()
                    if not cols:
                        return {"data": {"definition": "-- No DDL available"}}

                    col_lines = []
                    for col in cols:
                        col_name = str(col["column_name"]).replace('"', '""')
                        data_type = str(col["data_type"])
                        if col.get("character_maximum_length"):
                            data_type = f"{data_type}({col['character_maximum_length']})"
                        elif col.get("numeric_precision") and col.get("numeric_scale") is not None:
                            data_type = f"{data_type}({col['numeric_precision']},{col['numeric_scale']})"

                        not_null = " NOT NULL" if col.get("is_nullable") == "NO" else ""
                        default = f" DEFAULT {col['column_default']}" if col.get("column_default") else ""
                        col_lines.append(f'    "{col_name}" {data_type}{default}{not_null}')

                    schema_quoted = request.schema_name.replace('"', '""')
                    table_quoted = request.table.replace('"', '""')
                    definition = (
                        f'CREATE TABLE "{schema_quoted}"."{table_quoted}" (\n'
                        + ",\n".join(col_lines)
                        + "\n);"
                    )
                    return {"data": {"definition": definition}}

                elif explore_type == "stats":
                    if not request.schema_name or not request.table:
                        raise HTTPException(status_code=400, detail="schema and table are required")
                    cursor.execute(
                        "SELECT "
                        "c.reltuples::bigint AS estimated_rows, "
                        "pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size, "
                        "pg_size_pretty(pg_relation_size(c.oid)) AS table_size, "
                        "pg_size_pretty(pg_indexes_size(c.oid)) AS index_size, "
                        "st.n_live_tup AS live_tuples, "
                        "st.n_dead_tup AS dead_tuples, "
                        "st.last_analyze, "
                        "st.last_vacuum "
                        "FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid "
                        "WHERE n.nspname = %s AND c.relname = %s "
                        "LIMIT 1",
                        (request.schema_name, request.table),
                    )
                    row = cursor.fetchone()
                    return {"data": dict(row) if row else {}}

                else:
                    raise HTTPException(status_code=400, detail="Invalid explore type")
            else:
                raise HTTPException(
                    status_code=400, detail='Invalid action. Use "query" or "explore"'
                )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("PostgreSQL API Error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/postgres/info")
async def postgres_info(database: Optional[str] = None):
    """Return PostgreSQL server and database health info for UI status bar."""
    try:
        with get_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT current_setting('server_version') AS version")
            version_row = cursor.fetchone() or {}
            cursor.execute(
                "SELECT now() - pg_postmaster_start_time() AS uptime, "
                "COUNT(*) FILTER (WHERE datname = current_database())::int AS active_connections "
                "FROM pg_stat_activity"
            )
            runtime_row = cursor.fetchone() or {}
            cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size")
            size_row = cursor.fetchone() or {}

            return {
                "version": version_row.get("version"),
                "uptime": str(runtime_row.get("uptime", "")),
                "activeConnections": runtime_row.get("active_connections", 0),
                "databaseSize": size_row.get("database_size"),
                "state": "running",
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PostgreSQL info error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/postgres/queries")
async def postgres_list_queries(database: Optional[str] = None):
    """List active PostgreSQL queries for the current database."""
    try:
        with get_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT pid, UPPER(state) AS state, usename AS username, query, "
                "application_name AS \"applicationName\", "
                "CASE WHEN application_name LIKE 'openclaw:%' "
                "THEN split_part(application_name, ':', 2) ELSE NULL END AS \"queryTag\", "
                "to_char(now() - query_start, 'HH24:MI:SS.MS') AS \"elapsedTime\" "
                "FROM pg_stat_activity "
                "WHERE pid <> pg_backend_pid() "
                "AND datname = current_database() "
                "AND state IS NOT NULL "
                "AND state <> 'idle' "
                "AND query NOT ILIKE '%pg_stat_activity%' "
                "ORDER BY query_start DESC "
                "LIMIT 200"
            )
            return {"queries": [dict(row) for row in cursor.fetchall()]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PostgreSQL queries list error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/postgres/queries/{pid}")
async def postgres_kill_query(pid: int, database: Optional[str] = None):
    """Cancel a PostgreSQL backend query by PID (terminate as fallback)."""
    try:
        with get_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pg_cancel_backend(%s) AS cancelled", (pid,))
            cancel_row = cursor.fetchone() or {}
            if cancel_row.get("cancelled"):
                return {"status": "cancelled", "pid": pid}

            cursor.execute("SELECT pg_terminate_backend(%s) AS terminated", (pid,))
            row = cursor.fetchone() or {}
            if not row.get("terminated"):
                raise HTTPException(status_code=404, detail=f"Query process {pid} not found or not terminable")
            return {"status": "terminated", "pid": pid}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PostgreSQL kill query error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/postgres/sessions/{session_id}")
async def postgres_close_session(session_id: str):
    """Close a persisted PostgreSQL IDE session connection."""
    try:
        normalized = _normalize_token(session_id, "session_id")
        if normalized is None:
            raise HTTPException(status_code=400, detail="session_id is required")
        closed = _close_session(normalized)
        if not closed:
            raise HTTPException(status_code=404, detail=f"Session {normalized} not found")
        return {"status": "closed", "sessionId": normalized}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PostgreSQL session close error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
