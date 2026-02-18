"""
PostgreSQL Router — Execute queries and explore schema.
Ported from web-ui/src/app/api/postgres/route.ts
Uses psycopg v3 (modern PostgreSQL driver for Python).
"""
import logging
import time
from contextlib import contextmanager
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

router = APIRouter()

# ─── Connection Settings ───
PG_CONNINFO = "host=localhost port=5433 user=postgres password=Gs+163264128 dbname=controldb connect_timeout=5"


@contextmanager
def get_connection():
    """Get a PostgreSQL connection using psycopg v3."""
    conn = psycopg.connect(PG_CONNINFO, row_factory=dict_row, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


class PostgresRequest(BaseModel):
    action: str  # "query" or "explore"
    query: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table: Optional[str] = None
    type: Optional[str] = None  # for explore: databases, schemas, tables, columns

    def __init__(self, **data):
        # Map "schema" key from frontend to "schema_name"
        if "schema" in data and "schema_name" not in data:
            data["schema_name"] = data.pop("schema")
        super().__init__(**data)


@router.post("/postgres")
async def postgres_handler(request: PostgresRequest):
    """Execute a PostgreSQL query or explore schemas/tables."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            if request.action == "query":
                # ─── Execute SQL Query ───
                if not request.query:
                    raise HTTPException(status_code=400, detail="Query is required")

                start_time = time.time()
                cursor.execute(request.query)
                end_time = time.time()

                columns = [desc.name for desc in cursor.description] if cursor.description else []
                data = [dict(row) for row in cursor.fetchall()] if columns else []

                return {
                    "columns": columns,
                    "data": data,
                    "stats": {
                        "rowCount": cursor.rowcount,
                        "duration": int((end_time - start_time) * 1000),
                        "command": cursor.statusmessage,
                    },
                }

            elif request.action == "explore":
                # ─── Schema Exploration ───
                explore_type = request.type

                if explore_type == "databases":
                    cursor.execute(
                        "SELECT datname AS name FROM pg_database WHERE datistemplate = false ORDER BY datname"
                    )
                    return {"data": [dict(row) for row in cursor.fetchall()]}

                elif explore_type == "schemas":
                    if not request.database:
                        raise HTTPException(status_code=400, detail="database is required")
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
                        "SELECT table_name AS name, table_type "
                        "FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name",
                        (request.schema_name,),
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
