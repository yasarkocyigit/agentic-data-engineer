"""
Spark Router — Execute SQL queries and metadata exploration against Spark.

Uses a shared SparkSession connected to the configured Spark master.
"""
import asyncio
import logging
import os
import re
import threading
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "openclaw-sql")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
SPARK_DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

try:
    _MAX_RESULT_ROWS = max(1, int(os.getenv("SPARK_MAX_RESULT_ROWS", "10000")))
except ValueError:
    _MAX_RESULT_ROWS = 10000

_DB_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)
_SELECT_LIKE_PATTERN = re.compile(r"^\s*(SELECT|WITH|TABLE|VALUES)\b", re.IGNORECASE)

_spark_session = None
_spark_session_lock = threading.Lock()
_spark_query_lock = threading.Lock()


class SparkRequest(BaseModel):
    action: str = "query"  # "query" or "explore"
    query: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table: Optional[str] = None
    type: Optional[str] = None  # explore: databases, tables, columns
    limit: Optional[int] = Field(default=None, ge=1)

    def __init__(self, **data: Any):
        if "schema" in data and "schema_name" not in data:
            data["schema_name"] = data.pop("schema")
        super().__init__(**data)


def _normalize_database_name(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    db = value.strip()
    if not db:
        raise HTTPException(status_code=400, detail="database cannot be empty")
    if not _DB_NAME_PATTERN.match(db):
        raise HTTPException(status_code=400, detail="invalid database name")
    return db


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


def _get_spark_session():
    global _spark_session
    if _spark_session is not None:
        return _spark_session

    with _spark_session_lock:
        if _spark_session is not None:
            return _spark_session

        try:
            from pyspark.sql import SparkSession
        except ModuleNotFoundError as exc:
            raise HTTPException(
                status_code=500,
                detail="pyspark is not installed. Install it to enable Spark SQL engine.",
            ) from exc

        builder = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(SPARK_MASTER_URL)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
            .config("spark.ui.showConsoleProgress", "false")
        )
        if SPARK_DRIVER_HOST:
            builder = builder.config("spark.driver.host", SPARK_DRIVER_HOST)
        if SPARK_DRIVER_BIND_ADDRESS:
            builder = builder.config("spark.driver.bindAddress", SPARK_DRIVER_BIND_ADDRESS)

        _spark_session = builder.getOrCreate()
        logger.info("Spark session initialized. master=%s app=%s", SPARK_MASTER_URL, SPARK_APP_NAME)
        return _spark_session


def _row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, dict):
        return row
    return dict(row)


def _run_query_sync(request: SparkRequest) -> dict[str, Any]:
    spark = _get_spark_session()
    if not request.query:
        raise HTTPException(status_code=400, detail="Query is required")

    database = _normalize_database_name(request.database)
    sql_query, applied_limit = _apply_row_limit(request.query, request.limit)

    with _spark_query_lock:
        if database:
            spark.catalog.setCurrentDatabase(database)

        start = time.time()
        dataframe = spark.sql(sql_query)
        columns = [str(c) for c in dataframe.columns]
        truncated = False

        if columns:
            rows = dataframe.limit(_MAX_RESULT_ROWS + 1).collect()
            if len(rows) > _MAX_RESULT_ROWS:
                truncated = True
                rows = rows[:_MAX_RESULT_ROWS]
            data = [_row_to_dict(row) for row in rows]
            row_count = len(data)
        else:
            data = []
            row_count = 0

        duration_ms = int((time.time() - start) * 1000)
        current_db = spark.catalog.currentDatabase()

    payload: dict[str, Any] = {
        "columns": columns,
        "data": data,
        "stats": {
            "rowCount": row_count,
            "duration": duration_ms,
            "appliedLimit": applied_limit,
            "truncated": truncated,
            "maxRows": _MAX_RESULT_ROWS if columns else None,
            "database": current_db,
            "master": spark.sparkContext.master,
        },
    }
    if not columns:
        payload["message"] = "Statement executed successfully"
    return payload


def _explore_sync(request: SparkRequest) -> dict[str, Any]:
    spark = _get_spark_session()
    explore_type = (request.type or "").strip().lower()

    with _spark_query_lock:
        if explore_type == "databases":
            rows = spark.sql("SHOW DATABASES").collect()
            names: list[str] = []
            for row in rows:
                row_dict = _row_to_dict(row)
                name = (
                    row_dict.get("namespace")
                    or row_dict.get("databaseName")
                    or row_dict.get("database")
                )
                if isinstance(name, str) and name:
                    names.append(name)
            return {"data": [{"name": name} for name in names]}

        if explore_type == "tables":
            db = _normalize_database_name(request.schema_name or request.database)
            if not db:
                raise HTTPException(status_code=400, detail="schema/database is required")
            items = spark.catalog.listTables(db)
            data = []
            for item in items:
                table_type = str(getattr(item, "tableType", "TABLE") or "TABLE").upper()
                object_type = "view" if "VIEW" in table_type else "table"
                data.append({"name": item.name, "object_type": object_type})
            data.sort(key=lambda row: str(row["name"]).lower())
            return {"data": data}

        if explore_type == "columns":
            db = _normalize_database_name(request.schema_name or request.database)
            table = (request.table or "").strip()
            if not db or not table:
                raise HTTPException(status_code=400, detail="schema/database and table are required")
            cols = spark.catalog.listColumns(table, db)
            data = [{"name": c.name, "data_type": c.dataType} for c in cols]
            return {"data": data}

    raise HTTPException(status_code=400, detail="Invalid explore type")


@router.post("/spark")
async def spark_handler(request: SparkRequest):
    """Execute Spark SQL query or explore Spark metadata."""
    try:
        if request.action == "query":
            return await asyncio.to_thread(_run_query_sync, request)
        if request.action == "explore":
            return await asyncio.to_thread(_explore_sync, request)
        raise HTTPException(status_code=400, detail='Invalid action. Use "query" or "explore"')
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Spark API Error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/spark/info")
async def spark_info():
    """Return Spark session/runtime info for UI status bar."""
    try:
        spark = await asyncio.to_thread(_get_spark_session)
        default_db = await asyncio.to_thread(spark.catalog.currentDatabase)
        return {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "appName": spark.sparkContext.appName,
            "defaultDatabase": default_db,
            "state": "running",
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Spark info error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/spark/queries")
async def spark_queries():
    """
    Spark standalone SQL query-list API is connector-specific.
    Keep contract compatible with UI by returning an empty list.
    """
    return {"queries": []}
