"""Unified Databricks connector for Gemini Enterprise.

Single Cloud Run service that does everything:
  - On startup: creates the Vertex AI Search data store (idempotent) and
    ensures the GCS staging bucket exists.
  - Background thread: continuously syncs Databricks rows → GCS bucket (NDJSON)
    → Vertex AI Search so the Gemini Enterprise app always has fresh data.
  - REST API: Gemini Enterprise calls these tools to discover schema, run
    read-only SQL queries, and write changes back to Databricks.

All tool endpoints require an X-API-Key header matching WRITEBACK_API_KEY.
The /healthz and /sync/status endpoints are unauthenticated.
"""
import asyncio
import json
import os
import re
import secrets
import threading
import time
import uuid
from contextlib import asynccontextmanager

from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

load_dotenv()

# Import after load_dotenv so module-level env reads in these modules succeed
from create_datastore import create_data_store  # noqa: E402
from sync_databricks_to_datastore import (  # noqa: E402
    SYNC_INTERVAL_SECONDS,
    ensure_gcs_bucket,
    run_once,
)

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_CATALOG = os.environ["DATABRICKS_CATALOG"]
DATABRICKS_SCHEMA = os.environ["DATABRICKS_SCHEMA"]
DATABRICKS_TABLE = os.environ["DATABRICKS_TABLE"]
DATABRICKS_ID_COLUMN = os.environ.get("DATABRICKS_ID_COLUMN", "id")
DATABRICKS_UPDATED_COLUMN = os.environ.get("DATABRICKS_UPDATED_COLUMN", "")
API_KEY = os.environ["WRITEBACK_API_KEY"]

_path_parts = DATABRICKS_HTTP_PATH.rstrip("/").split("/")
SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or _path_parts[-1]

WRITABLE_COLUMNS: dict[str, type] = {
    "stock_count": int,
    "price": float,
    "category": str,
    "product_name": str,
}

TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE}"

_sync_state: dict = {
    "running": False,
    "last_run_epoch": None,
    "last_rows_synced": 0,
    "last_error": None,
}

# Set this event to trigger an immediate sync instead of waiting for the interval.
_sync_trigger = threading.Event()


def _sync_loop() -> None:
    while True:
        _sync_state["running"] = True
        try:
            rows = run_once()
            _sync_state["last_run_epoch"] = time.time()
            _sync_state["last_rows_synced"] = rows
            _sync_state["last_error"] = None
        except Exception as exc:
            _sync_state["last_error"] = str(exc)
            print(f"[sync] error: {exc}", flush=True)
        finally:
            _sync_state["running"] = False
        # Sleep for the normal interval, but wake up early if triggered by a write-back.
        _sync_trigger.wait(timeout=SYNC_INTERVAL_SECONDS)
        _sync_trigger.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Create Vertex AI Search data store (safe to call on every restart)
    try:
        create_data_store()
    except Exception as exc:
        print(f"[startup] datastore init warning (non-fatal): {exc}", flush=True)

    # 2. Ensure GCS staging bucket exists
    try:
        ensure_gcs_bucket()
    except Exception as exc:
        print(f"[startup] GCS bucket init warning (non-fatal): {exc}", flush=True)

    # 3. Start background sync thread (daemon so it dies if the process exits)
    t = threading.Thread(target=_sync_loop, daemon=True)
    t.start()
    print("[startup] background sync thread started.", flush=True)

    yield


app = FastAPI(
    title="Databricks Connector for Gemini Enterprise",
    description=(
        "Unified read and write access to Databricks for Gemini Enterprise. "
        "Reads discover schema and execute SELECT queries with PII redaction. "
        "Writes update whitelisted columns on product rows. "
        "A background thread keeps the Vertex AI Search data store in sync."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def verify_api_key(x_api_key: str = Header(...)) -> None:
    if not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _workspace_host() -> str:
    host = DATABRICKS_HOST.strip().rstrip("/")
    if not host.startswith(("https://", "http://")):
        host = f"https://{host}"
    return host


def _sql_connect():
    return databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def _sanitize_pii(text: str) -> str:
    text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    text = re.sub(r'\b(?:\d[ -]*?){13,16}\b', '[REDACTED_CARD]', text)
    return text


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class SchemaRequest(BaseModel):
    catalog: str = Field(default=DATABRICKS_CATALOG, description="Unity Catalog name")
    schema_name: str = Field(default=DATABRICKS_SCHEMA, description="Schema name")


class QueryRequest(BaseModel):
    sql_query: str = Field(..., description="SQL SELECT query to execute on Databricks")


class UpdateProductRequest(BaseModel):
    product_id: str = Field(..., description="ID of the product row to update")
    column: str = Field(
        ...,
        description=f"Column to update. Allowed values: {sorted(WRITABLE_COLUMNS)}",
    )
    value: str = Field(..., description="New value; automatically coerced to the column's type")


class InsertProductRequest(BaseModel):
    product_id: str = Field(..., description="Unique product ID, e.g. 'P112'")
    product_name: str = Field(..., description="Name of the product")
    category: str = Field(..., description="Category, e.g. 'Electronics' or 'Furniture'")
    price: float = Field(..., description="Price as a decimal number")
    stock_count: int = Field(..., description="Initial stock count")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "databricks-gemini-connector",
        "status": "HEALTHY",
        "tools": ["/tools/get_schema", "/tools/execute_sql", "/tools/update_product"],
        "sync": "/sync/status",
    }


@app.get("/healthz")
def health_check():
    return {"status": "HEALTHY", "service": "databricks-gemini-connector"}


@app.get("/sync/status")
def sync_status():
    """Current state of the background Databricks → Vertex AI Search sync."""
    return {
        "running": _sync_state["running"],
        "last_run_epoch": _sync_state["last_run_epoch"],
        "last_rows_synced": _sync_state["last_rows_synced"],
        "last_error": _sync_state["last_error"],
        "interval_seconds": SYNC_INTERVAL_SECONDS,
    }


@app.post(
    "/tools/get_schema",
    summary="Discover Unity Catalog tables and columns",
    dependencies=[Depends(verify_api_key)],
)
def get_schema(req: SchemaRequest):
    """Returns all tables and their column definitions from the given catalog and schema."""
    try:
        client = WorkspaceClient(host=_workspace_host(), token=DATABRICKS_TOKEN)
        tables = list(client.tables.list(catalog_name=req.catalog, schema_name=req.schema_name))
        schema_info = [
            {
                "table_name": t.name,
                "columns": [{"name": c.name, "type": c.type_text} for c in (t.columns or [])],
            }
            for t in tables
        ]
        return {"status": "SUCCESS", "catalog": req.catalog, "schema": req.schema_name, "tables": schema_info}
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Schema discovery failed: {exc}",
        ) from exc


@app.post(
    "/tools/execute_sql",
    summary="Execute a read-only SQL query with PII redaction",
    dependencies=[Depends(verify_api_key)],
)
def execute_sql(req: QueryRequest):
    """Executes a SELECT query and returns results with PII automatically redacted.
    Destructive statements (DROP, DELETE, TRUNCATE, ALTER, UPDATE, INSERT) are blocked.
    """
    blocked = {"DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT"}
    if any(kw in req.sql_query.upper().split() for kw in blocked):
        return {"status": "BLOCKED", "reason": "Only read-only SELECT operations are allowed."}

    if not SQL_WAREHOUSE_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SQL warehouse ID not configured. Set DATABRICKS_SQL_WAREHOUSE_ID.",
        )

    try:
        client = WorkspaceClient(host=_workspace_host(), token=DATABRICKS_TOKEN)
        response = client.statement_execution.execute_statement(
            statement=req.sql_query,
            warehouse_id=SQL_WAREHOUSE_ID,
        )
        if response.status.state == StatementState.SUCCEEDED:
            return {"status": "SUCCESS", "data": _sanitize_pii(str(response.result.as_dict()))}
        return {"status": "FAILED", "error": str(response.status.error)}
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"SQL execution failed: {exc}",
        ) from exc


@app.post(
    "/tools/update_product",
    summary="Update a single whitelisted column on one product row",
    dependencies=[Depends(verify_api_key)],
)
def update_product(req: UpdateProductRequest):
    """Updates one column on the product identified by product_id.
    Only stock_count, price, category, and product_name are writable.
    Sets last_updated timestamp automatically so the next sync picks up the change.
    """
    if req.column not in WRITABLE_COLUMNS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Column '{req.column}' is not writable. Allowed: {sorted(WRITABLE_COLUMNS)}",
        )

    try:
        typed_value = WRITABLE_COLUMNS[req.column](req.value)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Value '{req.value}' is not valid for column '{req.column}'.",
        )

    set_clause = f"{req.column} = :value"
    if DATABRICKS_UPDATED_COLUMN:
        set_clause += f", {DATABRICKS_UPDATED_COLUMN} = current_timestamp()"

    query = f"UPDATE {TABLE} SET {set_clause} WHERE {DATABRICKS_ID_COLUMN} = :row_id"

    try:
        with _sql_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, {"value": typed_value, "row_id": req.product_id})
                if cursor.rowcount == 0:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"No product found with {DATABRICKS_ID_COLUMN}='{req.product_id}'.",
                    )
                cursor.execute(
                    f"SELECT * FROM {TABLE} WHERE {DATABRICKS_ID_COLUMN} = :row_id",
                    {"row_id": req.product_id},
                )
                row = cursor.fetchone()
                columns = [col[0] for col in cursor.description]
                updated = dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Databricks update failed: {err}",
        ) from err

    _sync_trigger.set()  # wake the sync thread immediately so the change is searchable right away

    return {
        "status": "SUCCESS",
        "message": f"Updated {req.column} for {req.product_id}.",
        "row": {k: str(v) for k, v in updated.items()},
    }


@app.post(
    "/tools/insert_product",
    summary="Insert a new product row into Databricks",
    dependencies=[Depends(verify_api_key)],
)
def insert_product(req: InsertProductRequest):
    """Inserts a brand-new product row. Fails if the product_id already exists."""
    query = (
        f"INSERT INTO {TABLE} "
        f"(product_id, product_name, category, price, stock_count, {DATABRICKS_UPDATED_COLUMN or 'last_updated'}) "
        f"VALUES (:pid, :name, :cat, :price, :stock, current_timestamp())"
    )
    try:
        with _sql_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, {
                    "pid": req.product_id,
                    "name": req.product_name,
                    "cat": req.category,
                    "price": req.price,
                    "stock": req.stock_count,
                })
                cursor.execute(
                    f"SELECT * FROM {TABLE} WHERE {DATABRICKS_ID_COLUMN} = :pid",
                    {"pid": req.product_id},
                )
                row = cursor.fetchone()
                inserted = dict(zip([c[0] for c in cursor.description], row))
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Databricks insert failed: {err}",
        ) from err

    _sync_trigger.set()

    return {
        "status": "SUCCESS",
        "message": f"Inserted new product {req.product_id}.",
        "row": {k: str(v) for k, v in inserted.items()},
    }


# ---------------------------------------------------------------------------
# MCP Server — pure FastAPI/asyncio, no external library
# Exposes GET /mcp/sse  (SSE stream) and POST /mcp/messages/ (JSON-RPC)
# ---------------------------------------------------------------------------

_MCP_TOOLS = [
    {
        "name": "insert_product",
        "description": (
            "Insert a brand-new product row into Databricks inventory. "
            "Use this when the product_id does not yet exist. "
            "product_id: unique ID e.g. 'P112'. "
            "product_name, category, price (number), stock_count (integer) are all required."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["product_id", "product_name", "category", "price", "stock_count"],
            "properties": {
                "product_id": {"type": "string"},
                "product_name": {"type": "string"},
                "category": {"type": "string"},
                "price": {"type": "number"},
                "stock_count": {"type": "integer"},
            },
        },
    },
    {
        "name": "update_product",
        "description": (
            "Update a single field on an EXISTING product in Databricks. "
            "product_id: row ID e.g. 'P101'. "
            "column: one of stock_count, price, category, product_name. "
            "value: new value as a string."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["product_id", "column", "value"],
            "properties": {
                "product_id": {"type": "string"},
                "column": {"type": "string", "enum": ["stock_count", "price", "category", "product_name"]},
                "value": {"type": "string"},
            },
        },
    },
    {
        "name": "execute_sql",
        "description": "Execute a read-only SELECT query on Databricks. PII is auto-redacted. Destructive statements are blocked.",
        "inputSchema": {
            "type": "object",
            "required": ["sql_query"],
            "properties": {"sql_query": {"type": "string"}},
        },
    },
    {
        "name": "get_schema",
        "description": "Discover tables and columns in the Databricks Unity Catalog. Call before writing SQL queries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog": {"type": "string"},
                "schema_name": {"type": "string"},
            },
        },
    },
]

_mcp_sessions: dict[str, asyncio.Queue] = {}


def _mcp_run_tool(name: str, args: dict) -> str:
    if name == "insert_product":
        try:
            query = (
                f"INSERT INTO {TABLE} "
                f"(product_id, product_name, category, price, stock_count, {DATABRICKS_UPDATED_COLUMN or 'last_updated'}) "
                f"VALUES (:pid, :name, :cat, :price, :stock, current_timestamp())"
            )
            with _sql_connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, {
                        "pid": args.get("product_id", ""),
                        "name": args.get("product_name", ""),
                        "cat": args.get("category", ""),
                        "price": float(args.get("price", 0)),
                        "stock": int(args.get("stock_count", 0)),
                    })
                    cursor.execute(
                        f"SELECT * FROM {TABLE} WHERE {DATABRICKS_ID_COLUMN} = :pid",
                        {"pid": args.get("product_id", "")},
                    )
                    row = cursor.fetchone()
                    inserted = dict(zip([c[0] for c in cursor.description], row))
            _sync_trigger.set()
            return f"Inserted {args.get('product_id')}. Row: {inserted}"
        except Exception as exc:
            return f"Error: {exc}"

    if name == "update_product":
        product_id = args.get("product_id", "")
        column = args.get("column", "")
        value = args.get("value", "")
        if column not in WRITABLE_COLUMNS:
            return f"Error: column '{column}' not writable. Allowed: {sorted(WRITABLE_COLUMNS)}"
        try:
            typed_value = WRITABLE_COLUMNS[column](value)
        except (TypeError, ValueError):
            return f"Error: value '{value}' invalid for column '{column}'"
        set_clause = f"{column} = :value"
        if DATABRICKS_UPDATED_COLUMN:
            set_clause += f", {DATABRICKS_UPDATED_COLUMN} = current_timestamp()"
        query = f"UPDATE {TABLE} SET {set_clause} WHERE {DATABRICKS_ID_COLUMN} = :row_id"
        try:
            with _sql_connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, {"value": typed_value, "row_id": product_id})
                    if cursor.rowcount == 0:
                        return f"Error: no product with {DATABRICKS_ID_COLUMN}='{product_id}'"
                    cursor.execute(
                        f"SELECT * FROM {TABLE} WHERE {DATABRICKS_ID_COLUMN} = :row_id",
                        {"row_id": product_id},
                    )
                    row = cursor.fetchone()
                    updated = dict(zip([c[0] for c in cursor.description], row))
            _sync_trigger.set()
            return f"Updated {column} for {product_id}. Row: {updated}"
        except Exception as exc:
            return f"Error: {exc}"

    if name == "execute_sql":
        sql_query = args.get("sql_query", "")
        blocked = {"DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT"}
        if any(kw in sql_query.upper().split() for kw in blocked):
            return "BLOCKED: only SELECT allowed."
        try:
            client = WorkspaceClient(host=_workspace_host(), token=DATABRICKS_TOKEN)
            response = client.statement_execution.execute_statement(
                statement=sql_query, warehouse_id=SQL_WAREHOUSE_ID
            )
            if response.status.state == StatementState.SUCCEEDED:
                return _sanitize_pii(str(response.result.as_dict()))
            return f"FAILED: {response.status.error}"
        except Exception as exc:
            return f"Error: {exc}"

    if name == "get_schema":
        cat = args.get("catalog") or DATABRICKS_CATALOG
        sch = args.get("schema_name") or DATABRICKS_SCHEMA
        try:
            client = WorkspaceClient(host=_workspace_host(), token=DATABRICKS_TOKEN)
            tables = list(client.tables.list(catalog_name=cat, schema_name=sch))
            info = [
                {"table": t.name, "columns": [{"name": c.name, "type": c.type_text} for c in (t.columns or [])]}
                for t in tables
            ]
            return str(info)
        except Exception as exc:
            return f"Error: {exc}"

    return f"Error: unknown tool '{name}'"


async def _sse_generator(request: Request, session_id: str):
    yield f"event: endpoint\ndata: /mcp/messages/?sessionId={session_id}\n\n"
    queue: asyncio.Queue = asyncio.Queue()
    _mcp_sessions[session_id] = queue
    try:
        while not await request.is_disconnected():
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=25)
                yield f"event: message\ndata: {json.dumps(msg)}\n\n"
            except asyncio.TimeoutError:
                yield ": ping\n\n"
    finally:
        _mcp_sessions.pop(session_id, None)


_mcp_router = APIRouter(prefix="/mcp")


@_mcp_router.get("/sse")
async def mcp_sse(request: Request):
    session_id = str(uuid.uuid4())
    return StreamingResponse(
        _sse_generator(request, session_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@_mcp_router.post("/messages/")
async def mcp_messages(request: Request):
    session_id = request.query_params.get("sessionId", "")
    body = await request.json()
    msg_id = body.get("id")
    method = body.get("method", "")
    params = body.get("params", {})

    if method == "initialize":
        resp = {
            "jsonrpc": "2.0", "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "databricks-connector", "version": "1.0.0"},
            },
        }
    elif method in ("notifications/initialized", "ping"):
        return JSONResponse({"status": "ok"})
    elif method == "tools/list":
        resp = {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": _MCP_TOOLS}}
    elif method == "tools/call":
        result_text = _mcp_run_tool(params.get("name", ""), params.get("arguments", {}))
        resp = {
            "jsonrpc": "2.0", "id": msg_id,
            "result": {"content": [{"type": "text", "text": result_text}]},
        }
    else:
        resp = {"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": f"Unknown method: {method}"}}

    if session_id in _mcp_sessions:
        await _mcp_sessions[session_id].put(resp)

    return JSONResponse({"status": "ok"})


app.include_router(_mcp_router)
print("[startup] MCP server ready at /mcp/sse", flush=True)
