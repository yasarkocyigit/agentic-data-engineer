"""
Notebook Router — Interactive code execution via Jupyter Kernel Gateway.
Provides kernel session management and cell execution for the Notebook UI.

Falls back to local subprocess execution when Kernel Gateway is unavailable.
"""
import asyncio
import json
import logging
import os
import uuid
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

KERNEL_GW_URL = os.getenv("JUPYTER_KERNEL_GW_URL", "http://localhost:8888")
NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "notebooks"))

# Ensure notebooks directory exists
Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)


# ─── Models ───

class ExecuteRequest(BaseModel):
    code: str
    kernel_id: Optional[str] = None


class NotebookCell(BaseModel):
    id: str
    cell_type: str = "code"  # "code" or "markdown"
    source: str = ""
    language: str = "python"  # "python" or "sql"
    outputs: list = []
    execution_count: Optional[int] = None


class SaveNotebookRequest(BaseModel):
    name: str
    cells: list[dict]


# ─── In-Memory Session Store ───
# Maps session_id -> { kernel_id, created_at, last_used }
_sessions: dict[str, dict] = {}
_execution_counts: dict[str, int] = {}


# ─── Helpers ───

async def _kernel_gw_available() -> bool:
    """Check if Jupyter Kernel Gateway is reachable."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{KERNEL_GW_URL}/api")
            return resp.status_code == 200
    except Exception:
        return False


async def _kg_request(method: str, path: str, **kwargs) -> dict:
    """Make a request to the Kernel Gateway API."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(method, f"{KERNEL_GW_URL}{path}", **kwargs)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json() if resp.text else {}


async def _execute_via_gateway(kernel_id: str, code: str) -> dict:
    """Execute code via Kernel Gateway WebSocket protocol."""
    import websockets  # type: ignore

    ws_url = f"ws://{KERNEL_GW_URL.replace('http://', '')}/api/kernels/{kernel_id}/channels"

    msg_id = str(uuid.uuid4())
    execute_msg = {
        "header": {
            "msg_id": msg_id,
            "msg_type": "execute_request",
            "username": "openclaw",
            "session": str(uuid.uuid4()),
            "date": datetime.utcnow().isoformat() + "Z",
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
            "stop_on_error": True,
        },
        "buffers": [],
        "channel": "shell",
    }

    outputs = []
    status = "ok"
    execution_count = None

    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps(execute_msg))

            # Collect responses until we get execute_reply
            deadline = time.time() + 60  # 60s timeout
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    msg = json.loads(raw)
                except asyncio.TimeoutError:
                    break

                msg_type = msg.get("msg_type", "")
                parent_id = msg.get("parent_header", {}).get("msg_id", "")

                if parent_id != msg_id:
                    continue

                if msg_type == "stream":
                    outputs.append({
                        "output_type": "stream",
                        "name": msg["content"].get("name", "stdout"),
                        "text": msg["content"].get("text", ""),
                    })
                elif msg_type == "execute_result":
                    execution_count = msg["content"].get("execution_count")
                    data = msg["content"].get("data", {})
                    outputs.append({
                        "output_type": "execute_result",
                        "data": data,
                        "execution_count": execution_count,
                    })
                elif msg_type == "display_data":
                    outputs.append({
                        "output_type": "display_data",
                        "data": msg["content"].get("data", {}),
                    })
                elif msg_type == "error":
                    status = "error"
                    outputs.append({
                        "output_type": "error",
                        "ename": msg["content"].get("ename", "Error"),
                        "evalue": msg["content"].get("evalue", ""),
                        "traceback": msg["content"].get("traceback", []),
                    })
                elif msg_type == "execute_reply":
                    status = msg["content"].get("status", "ok")
                    execution_count = msg["content"].get("execution_count")
                    break

    except Exception as e:
        logger.error(f"WebSocket execution error: {e}")
        status = "error"
        outputs.append({
            "output_type": "error",
            "ename": "ConnectionError",
            "evalue": str(e),
            "traceback": [],
        })

    return {
        "status": status,
        "execution_count": execution_count,
        "outputs": outputs,
    }


def _sanitize_sql(raw_sql: str) -> str:
    """Clean SQL for Trino: strip comments, trailing semicolons, and extra whitespace."""
    lines = raw_sql.split('\n')
    cleaned_lines = []
    for line in lines:
        # Remove single-line comments (-- ...)
        comment_idx = line.find('--')
        if comment_idx >= 0:
            line = line[:comment_idx]
        line = line.rstrip()
        if line:
            cleaned_lines.append(line)
    sql = ' '.join(cleaned_lines).strip()
    # Remove trailing semicolons (Trino doesn't accept them)
    while sql.endswith(';'):
        sql = sql[:-1].strip()
    return sql


def _execute_local(code: str, session_id: str) -> dict:
    """Fallback: Execute Python code via local subprocess."""
    _execution_counts.setdefault(session_id, 0)
    _execution_counts[session_id] += 1
    exec_count = _execution_counts[session_id]

    trino_host = os.getenv("TRINO_HOST", "localhost")
    trino_port = os.getenv("TRINO_PORT", "8083")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access = os.getenv("MINIO_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
    minio_secret = os.getenv("MINIO_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "admin123"))

    # Detect SQL magic
    stripped = code.strip()
    if stripped.startswith("%%sql"):
        raw_sql = stripped[len("%%sql"):].strip()
        sql = _sanitize_sql(raw_sql)
        # Escape single quotes for Python string embedding
        sql_escaped = sql.replace("'", "\\'")
        code = f"""
import trino.dbapi as trino_db
import json
from decimal import Decimal
from datetime import datetime, date

class _TrinoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        return super().default(obj)

conn = trino_db.connect(host='{trino_host}', port={trino_port}, user='admin', catalog='iceberg', schema='default')
cur = conn.cursor()
cur.execute('{sql_escaped}')
rows = cur.fetchall()
cols = [d[0] for d in cur.description] if cur.description else []
print("__TABLE_START__")
print(json.dumps({{"columns": cols, "rows": [list(r) for r in rows[:500]]}}, cls=_TrinoEncoder))
print("__TABLE_END__")
conn.close()
"""
    else:
        # For Python cells — comprehensive Databricks-like preamble
        preamble = f"""
import os as _os, json as _json, sys as _sys
_os.environ.setdefault('MINIO_ENDPOINT', '{minio_endpoint}')
_os.environ.setdefault('AWS_ACCESS_KEY_ID', '{minio_access}')
_os.environ.setdefault('AWS_SECRET_ACCESS_KEY', '{minio_secret}')

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs as _s3fs
import trino.dbapi as _trino_db

# ─── Storage FileSystem ───
_fs = _s3fs.S3FileSystem(
    key='{minio_access}', secret='{minio_secret}',
    endpoint_url='{minio_endpoint}', use_ssl=False,
)

# ─── Trino Connection ───
def _get_trino_conn():
    return _trino_db.connect(host='{trino_host}', port={trino_port}, user='admin', catalog='iceberg', schema='default')

def query_trino(sql):
    \"\"\"Execute SQL via Trino, return (columns, rows).\"\"\"
    c = _get_trino_conn(); cur = c.cursor(); cur.execute(sql)
    rows = cur.fetchall(); cols = [d[0] for d in cur.description] if cur.description else []
    c.close(); return cols, rows

def query(sql):
    \"\"\"Execute SQL via Trino → pandas DataFrame.\"\"\"
    cols, rows = query_trino(sql)
    return pd.DataFrame(rows, columns=cols)

def sql(statement):
    \"\"\"Execute a Trino DDL/DML statement (CREATE TABLE, INSERT, DROP, etc).\"\"\"
    c = _get_trino_conn(); cur = c.cursor(); cur.execute(statement)
    try:
        rows = cur.fetchall(); cols = [d[0] for d in cur.description] if cur.description else []
        c.close(); return pd.DataFrame(rows, columns=cols) if cols else None
    except Exception:
        c.close(); return None

# ─── Read Helpers ───
def read_parquet(path, **kw):
    \"\"\"Read parquet from MinIO → DataFrame. Example: read_parquet('silver/clean_orders')\"\"\"
    return pq.read_table(path, filesystem=_fs, **kw).to_pandas()

def read_csv(path, **kw):
    \"\"\"Read CSV from MinIO → DataFrame. Example: read_csv('bronze/data.csv')\"\"\"
    with _fs.open(path, 'r') as f: return pd.read_csv(f, **kw)

def read_json(path, **kw):
    \"\"\"Read JSON/JSONL from MinIO → DataFrame.\"\"\"
    with _fs.open(path, 'r') as f: return pd.read_json(f, **kw)

def read_delta(path):
    \"\"\"Read a Delta table from MinIO → DataFrame. Example: read_delta('silver/orders_delta')\"\"\"
    from deltalake import DeltaTable as _DT
    dt = _DT(path, storage_options={{
        'AWS_ACCESS_KEY_ID': '{minio_access}', 'AWS_SECRET_ACCESS_KEY': '{minio_secret}',
        'AWS_ENDPOINT_URL': '{minio_endpoint}', 'AWS_ALLOW_HTTP': 'true',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true', 'AWS_REGION': 'us-east-1',
    }})
    return dt.to_pandas()

def read_iceberg(table_name, limit=None):
    \"\"\"Read an Iceberg table via Trino → DataFrame. Example: read_iceberg('silver.clean_orders')\"\"\"
    q = f'SELECT * FROM iceberg.{{table_name}}'
    if limit: q += f' LIMIT {{limit}}'
    return query(q)

# ─── Write Helpers ───
def write_parquet(df, path, **kw):
    \"\"\"Write DataFrame as parquet to MinIO. Example: write_parquet(df, 'silver/my_table/')\"\"\"
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=path, filesystem=_fs, **kw)
    print(f"✅ Written {{len(df)}} rows as parquet → {{path}}")

def write_csv(df, path, **kw):
    \"\"\"Write DataFrame as CSV to MinIO.\"\"\"
    with _fs.open(path, 'w') as f: df.to_csv(f, index=False, **kw)
    print(f"✅ Written {{len(df)}} rows as CSV → {{path}}")

def write_delta(df, path, mode='overwrite', **kw):
    \"\"\"Write DataFrame as Delta table to MinIO.
    mode: 'overwrite', 'append', 'error', 'ignore'
    Example: write_delta(df, 's3a://silver/orders_delta/')
    \"\"\"
    from deltalake import write_deltalake as _wd
    _storage = {{
        'AWS_ACCESS_KEY_ID': '{minio_access}', 'AWS_SECRET_ACCESS_KEY': '{minio_secret}',
        'AWS_ENDPOINT_URL': '{minio_endpoint}', 'AWS_ALLOW_HTTP': 'true',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true', 'AWS_REGION': 'us-east-1',
    }}
    _wd(path, df, mode=mode, storage_options=_storage, **kw)
    print(f"✅ Written {{len(df)}} rows as Delta ({{mode}}) → {{path}}")

def write_iceberg(df, table_name, mode='overwrite'):
    \"\"\"Write DataFrame to Iceberg table via Trino.
    Creates the table if it doesn't exist (CTAS), or INSERT INTO for append.
    table_name: e.g. 'silver.my_table' (resolved under iceberg catalog)
    mode: 'overwrite' or 'append'

    Example: write_iceberg(df, 'silver.my_analysis')
    \"\"\"
    full = f'iceberg.{{table_name}}'
    # Write to temp parquet, then use Trino CTAS/INSERT
    tmp_path = f'warehouse/_tmp_{{table_name.replace(".", "_")}}'
    write_parquet(df, tmp_path)

    if mode == 'overwrite':
        sql(f'DROP TABLE IF EXISTS {{full}}')
        # Build column defs from DataFrame
        _type_map = {{'int64': 'BIGINT', 'float64': 'DOUBLE', 'object': 'VARCHAR', 'bool': 'BOOLEAN',
                      'datetime64[ns]': 'TIMESTAMP', 'int32': 'INTEGER', 'float32': 'REAL'}}
        col_defs = ', '.join(f'\"{{c}}\" {{_type_map.get(str(df[c].dtype), "VARCHAR")}}' for c in df.columns)
        sql(f'CREATE TABLE {{full}} ({{col_defs}})')
        # Insert via VALUES (batch of 1000)
        for i in range(0, len(df), 1000):
            batch = df.iloc[i:i+1000]
            vals = ', '.join('(' + ', '.join(
                'NULL' if pd.isna(v) else f"'{{str(v).replace(chr(39), chr(39)+chr(39))}}'" if isinstance(v, str) else str(v)
                for v in row) + ')' for _, row in batch.iterrows())
            sql(f'INSERT INTO {{full}} VALUES {{vals}}')
        # Cleanup temp
        try: _fs.rm(tmp_path, recursive=True)
        except: pass
        print(f"✅ Written {{len(df)}} rows to Iceberg table {{full}} (overwrite)")
    else:
        # Append mode
        for i in range(0, len(df), 1000):
            batch = df.iloc[i:i+1000]
            vals = ', '.join('(' + ', '.join(
                'NULL' if pd.isna(v) else f"'{{str(v).replace(chr(39), chr(39)+chr(39))}}'" if isinstance(v, str) else str(v)
                for v in row) + ')' for _, row in batch.iterrows())
            sql(f'INSERT INTO {{full}} VALUES {{vals}}')
        try: _fs.rm(tmp_path, recursive=True)
        except: pass
        print(f"✅ Appended {{len(df)}} rows to Iceberg table {{full}}")

# ─── Utility Helpers ───
def list_files(path='', detail=False):
    \"\"\"List files/dirs in MinIO. Example: list_files('silver/')\"\"\"
    return _fs.ls(path, detail=detail)

def list_tables(schema='silver'):
    \"\"\"List Iceberg tables in a schema.\"\"\"
    return query(f'SHOW TABLES FROM iceberg.{{schema}}')

def describe(table_name):
    \"\"\"Describe an Iceberg table. Example: describe('silver.clean_orders')\"\"\"
    return query(f'DESCRIBE iceberg.{{table_name}}')

class _DfEncoder(_json.JSONEncoder):
    def default(self, obj):
        import pandas as pd
        if pd.isna(obj): return None
        if hasattr(obj, 'isoformat'): return obj.isoformat()
        try:
            from decimal import Decimal
            if isinstance(obj, Decimal): return float(obj)
            import numpy as np
            if isinstance(obj, (np.integer, np.floating)): return obj.item()
        except: pass
        return str(obj)

def display(df, n=20):
    \"\"\"Pretty-print a DataFrame (Databricks-style).\"\"\"
    if isinstance(df, pd.DataFrame):
        _cols = list(df.columns)
        _df_head = df.head(n)
        _rows = _df_head.where(pd.notnull(_df_head), None).values.tolist()
        print("__TABLE_START__")
        print(_json.dumps({{"columns": _cols, "rows": _rows}}, cls=_DfEncoder))
        print("__TABLE_END__")
        if len(df) > n:
            print(f"\\n[Showing {{n}} of {{len(df)}} rows x {{len(df.columns)}} columns]")
        else:
            print(f"\\n[{{len(df)}} rows x {{len(df.columns)}} columns]")
    else:
        print(df)

def show_schemas():
    \"\"\"List all schemas in the Iceberg catalog.\"\"\"
    return query('SHOW SCHEMAS FROM iceberg')

def create_schema(name):
    \"\"\"Create a new schema in Iceberg. Example: create_schema('gold')\"\"\"
    sql(f'CREATE SCHEMA IF NOT EXISTS iceberg.{{name}}')
    print(f"✅ Schema iceberg.{{name}} ready")
"""
        code = preamble + "\n" + code

    outputs = []
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            f.flush()
            result = subprocess.run(
                [sys.executable, f.name],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=NOTEBOOKS_DIR,
            )
            os.unlink(f.name)

        stdout = result.stdout
        stderr = result.stderr

        # Check for table output
        if "__TABLE_START__" in stdout and "__TABLE_END__" in stdout:
            before = stdout[:stdout.index("__TABLE_START__")]
            table_json = stdout[stdout.index("__TABLE_START__") + len("__TABLE_START__"):stdout.index("__TABLE_END__")].strip()
            after = stdout[stdout.index("__TABLE_END__") + len("__TABLE_END__"):]

            if before.strip():
                outputs.append({"output_type": "stream", "name": "stdout", "text": before})
            try:
                table_data = json.loads(table_json)
                outputs.append({
                    "output_type": "execute_result",
                    "data": {"application/json": table_data, "text/plain": table_json},
                    "execution_count": exec_count,
                })
            except json.JSONDecodeError:
                outputs.append({"output_type": "stream", "name": "stdout", "text": table_json})
            if after.strip():
                outputs.append({"output_type": "stream", "name": "stdout", "text": after})
        elif stdout:
            outputs.append({"output_type": "stream", "name": "stdout", "text": stdout})

        if stderr:
            outputs.append({"output_type": "stream", "name": "stderr", "text": stderr})

        status = "ok" if result.returncode == 0 else "error"
        if result.returncode != 0 and not stderr:
            outputs.append({
                "output_type": "error",
                "ename": "ProcessError",
                "evalue": f"Process exited with code {result.returncode}",
                "traceback": [],
            })

    except subprocess.TimeoutExpired:
        status = "error"
        outputs.append({
            "output_type": "error",
            "ename": "TimeoutError",
            "evalue": "Cell execution timed out after 120 seconds",
            "traceback": [],
        })
    except Exception as e:
        status = "error"
        outputs.append({
            "output_type": "error",
            "ename": type(e).__name__,
            "evalue": str(e),
            "traceback": [],
        })

    return {
        "status": status,
        "execution_count": exec_count,
        "outputs": outputs,
    }


# ─── Routes: Kernel Management ───

@router.get("/notebook/kernels")
async def list_kernels():
    """List active kernel sessions."""
    gw_available = await _kernel_gw_available()
    if gw_available:
        try:
            kernels = await _kg_request("GET", "/api/kernels")
            return {"kernels": kernels, "mode": "gateway"}
        except Exception:
            pass

    # Return local sessions
    return {
        "kernels": [
            {"id": sid, "name": "python3", "execution_state": "idle", **info}
            for sid, info in _sessions.items()
        ],
        "mode": "local",
    }


@router.post("/notebook/kernels")
async def start_kernel():
    """Start a new kernel session."""
    gw_available = await _kernel_gw_available()

    if gw_available:
        try:
            result = await _kg_request("POST", "/api/kernels", json={"name": "python3"})
            session_id = result.get("id", str(uuid.uuid4()))
            _sessions[session_id] = {
                "kernel_id": result.get("id"),
                "created_at": datetime.utcnow().isoformat(),
                "mode": "gateway",
            }
            return {"id": session_id, "mode": "gateway", "status": "started"}
        except Exception as e:
            logger.warning(f"Kernel Gateway unavailable, falling back to local: {e}")

    # Local fallback
    session_id = str(uuid.uuid4())
    _sessions[session_id] = {
        "kernel_id": session_id,
        "created_at": datetime.utcnow().isoformat(),
        "mode": "local",
    }
    _execution_counts[session_id] = 0
    return {"id": session_id, "mode": "local", "status": "started"}


@router.delete("/notebook/kernels/{kernel_id}")
async def shutdown_kernel(kernel_id: str):
    """Shutdown a kernel session."""
    session = _sessions.pop(kernel_id, None)
    if session and session.get("mode") == "gateway":
        try:
            await _kg_request("DELETE", f"/api/kernels/{session['kernel_id']}")
        except Exception:
            pass

    _execution_counts.pop(kernel_id, None)
    return {"status": "shutdown", "id": kernel_id}


@router.post("/notebook/kernels/{kernel_id}/restart")
async def restart_kernel(kernel_id: str):
    """Restart a kernel session."""
    session = _sessions.get(kernel_id)
    if session and session.get("mode") == "gateway":
        try:
            await _kg_request("POST", f"/api/kernels/{session['kernel_id']}/restart")
            return {"status": "restarted", "mode": "gateway"}
        except Exception:
            pass

    # Reset local execution count
    _execution_counts[kernel_id] = 0
    return {"status": "restarted", "mode": "local"}


@router.post("/notebook/kernels/{kernel_id}/interrupt")
async def interrupt_kernel(kernel_id: str):
    """Interrupt a running execution."""
    session = _sessions.get(kernel_id)
    if session and session.get("mode") == "gateway":
        try:
            await _kg_request("POST", f"/api/kernels/{session['kernel_id']}/interrupt")
            return {"status": "interrupted"}
        except Exception:
            pass
    return {"status": "interrupted"}


# ─── Routes: Cell Execution ───

@router.post("/notebook/kernels/{kernel_id}/execute")
async def execute_cell(kernel_id: str, req: ExecuteRequest):
    """Execute a code cell and return outputs."""
    session = _sessions.get(kernel_id)

    if session and session.get("mode") == "gateway":
        try:
            result = await _execute_via_gateway(session["kernel_id"], req.code)
            return result
        except Exception as e:
            logger.warning(f"Gateway execution failed, falling back to local: {e}")

    # Local fallback
    if kernel_id not in _sessions:
        _sessions[kernel_id] = {
            "kernel_id": kernel_id,
            "created_at": datetime.utcnow().isoformat(),
            "mode": "local",
        }

    result = _execute_local(req.code, kernel_id)
    return result


# ─── Routes: Notebook File Management ───

@router.get("/notebook/files")
async def list_notebooks():
    """List saved notebook files."""
    notebooks = []
    nb_dir = Path(NOTEBOOKS_DIR)
    for f in sorted(nb_dir.glob("*.ipynb")):
        stat = f.stat()
        notebooks.append({
            "name": f.stem,
            "filename": f.name,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })

    # Also list .json notebook files (our simple format)
    for f in sorted(nb_dir.glob("*.notebook.json")):
        stat = f.stat()
        notebooks.append({
            "name": f.name.replace(".notebook.json", ""),
            "filename": f.name,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })

    return {"notebooks": notebooks}


@router.post("/notebook/files")
async def save_notebook(req: SaveNotebookRequest):
    """Save a notebook (cells + outputs)."""
    safe_name = "".join(c for c in req.name if c.isalnum() or c in "-_ ").strip()
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid notebook name")

    filepath = Path(NOTEBOOKS_DIR) / f"{safe_name}.notebook.json"
    data = {
        "name": safe_name,
        "created": datetime.utcnow().isoformat(),
        "cells": req.cells,
    }
    filepath.write_text(json.dumps(data, indent=2))
    return {"status": "saved", "filename": filepath.name}


@router.get("/notebook/files/{name}")
async def load_notebook(name: str):
    """Load a saved notebook."""
    # Try our format first
    filepath = Path(NOTEBOOKS_DIR) / f"{name}.notebook.json"
    if filepath.exists():
        data = json.loads(filepath.read_text())
        return data

    # Try .ipynb format
    filepath_ipynb = Path(NOTEBOOKS_DIR) / f"{name}.ipynb"
    if filepath_ipynb.exists():
        data = json.loads(filepath_ipynb.read_text())
        # Convert ipynb to our format
        cells = []
        for cell in data.get("cells", []):
            cells.append({
                "id": str(uuid.uuid4()),
                "cell_type": cell.get("cell_type", "code"),
                "source": "".join(cell.get("source", [])),
                "language": "python",
                "outputs": cell.get("outputs", []),
                "execution_count": cell.get("execution_count"),
            })
        return {"name": name, "cells": cells}

    raise HTTPException(status_code=404, detail=f"Notebook '{name}' not found")


@router.delete("/notebook/files/{name}")
async def delete_notebook(name: str):
    """Delete a saved notebook."""
    for ext in [".notebook.json", ".ipynb"]:
        filepath = Path(NOTEBOOKS_DIR) / f"{name}{ext}"
        if filepath.exists():
            filepath.unlink()
            return {"status": "deleted", "name": name}
    raise HTTPException(status_code=404, detail=f"Notebook '{name}' not found")


# ─── Routes: Health ───

@router.get("/notebook/health")
async def notebook_health():
    """Check notebook execution engine status."""
    gw_available = await _kernel_gw_available()
    return {
        "gateway_available": gw_available,
        "gateway_url": KERNEL_GW_URL,
        "mode": "gateway" if gw_available else "local",
        "active_sessions": len(_sessions),
        "notebooks_dir": str(NOTEBOOKS_DIR),
    }
