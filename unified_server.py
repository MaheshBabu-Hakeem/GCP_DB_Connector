"""Unified Databricks connector for Gemini Enterprise.

Single API surface for reads (schema discovery, SQL queries) and
writes (product row updates). All endpoints share one auth method:
X-API-Key header matching WRITEBACK_API_KEY in the environment.

Reads use the Databricks SDK (WorkspaceClient) for schema discovery
and SQL execution. Writes use the SQL connector for transactional UPDATE.
PII in query results is automatically redacted before returning.
"""
import os
import re
import secrets

from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

load_dotenv()

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_CATALOG = os.environ["DATABRICKS_CATALOG"]
DATABRICKS_SCHEMA = os.environ["DATABRICKS_SCHEMA"]
DATABRICKS_TABLE = os.environ["DATABRICKS_TABLE"]
DATABRICKS_ID_COLUMN = os.environ.get("DATABRICKS_ID_COLUMN", "id")
DATABRICKS_UPDATED_COLUMN = os.environ.get("DATABRICKS_UPDATED_COLUMN", "")
API_KEY = os.environ["WRITEBACK_API_KEY"]

# Derive warehouse ID from HTTP path (/sql/1.0/warehouses/<id>) if not set explicitly.
_path_parts = DATABRICKS_HTTP_PATH.rstrip("/").split("/")
SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or _path_parts[-1]

# Only these columns may be written — anything else is rejected so the model
# can never steer an UPDATE at an arbitrary column.
WRITABLE_COLUMNS: dict[str, type] = {
    "stock_count": int,
    "price": float,
    "category": str,
    "product_name": str,
}

TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE}"

app = FastAPI(
    title="Databricks Connector for Gemini Enterprise",
    description=(
        "Unified read and write access to Databricks for Gemini Enterprise agents. "
        "Reads discover schema and execute SELECT queries with PII redaction. "
        "Writes update whitelisted columns on product rows."
    ),
    version="1.0.0",
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "databricks-gemini-connector",
        "status": "HEALTHY",
        "tools": ["/tools/get_schema", "/tools/execute_sql", "/tools/update_product"],
    }


@app.get("/healthz")
def health_check():
    return {"status": "HEALTHY", "service": "databricks-gemini-connector"}


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
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Schema discovery failed: {e}",
        ) from e


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
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"SQL execution failed: {e}",
        ) from e


@app.post(
    "/tools/update_product",
    summary="Update a single whitelisted column on one product row",
    dependencies=[Depends(verify_api_key)],
)
def update_product(req: UpdateProductRequest):
    """Updates one column on the product identified by product_id.
    Only stock_count, price, category, and product_name are writable.
    Sets last_updated timestamp automatically so the sync job picks up the change.
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

    return {
        "status": "SUCCESS",
        "message": f"Updated {req.column} for {req.product_id}.",
        "row": {k: str(v) for k, v in updated.items()},
    }
