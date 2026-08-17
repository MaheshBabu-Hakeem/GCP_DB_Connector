import os
import re
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

app = FastAPI(
    title="Databricks Enterprise Connector for Gemini",
    description="Bridge API enabling Gemini to query Databricks with RBAC and PII masking.",
    version="1.0.0"
)

load_dotenv()

# Load configurations from environment
DATABRICKS_HOST = "https://dbc-c486d425-ef89.cloud.databricks.com"
SQL_WAREHOUSE_ID = "aa7b310aa23458d2"


def workspace_host() -> str:
    if not DATABRICKS_HOST:
        raise RuntimeError("DATABRICKS_HOST is not configured.")

    host = DATABRICKS_HOST.strip().rstrip("/")
    warehouse_path = "/sql/1.0/warehouses/"
    if warehouse_path in host:
        raise RuntimeError(
            "DATABRICKS_HOST must be the workspace URL, for example "
            "https://dbc-1234567890123456.cloud.databricks.com, not the SQL "
            "warehouse HTTP path. Put the warehouse ID in "
            "DATABRICKS_SQL_WAREHOUSE_ID."
        )
    if not host.startswith(("https://", "http://")):
        raise RuntimeError(
            "DATABRICKS_HOST must start with https:// and contain only the "
            "Databricks workspace host."
        )
    return host


def get_token(authorization: str) -> str:
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization must be provided as: Bearer <Databricks PAT>.",
        )
    return token.strip()


def databricks_error(prefix: str, error: Exception) -> HTTPException:
    message = str(error).strip() or error.__class__.__name__
    return HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail=f"{prefix}: {message}",
    )


# --- DATA GUARDRAIL FUNCTION ---
def sanitize_pii(text: str) -> str:
    """Masks sensitive data (emails, SSNs, credit cards) before sending to Gemini."""
    text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    text = re.sub(r'\b(?:\d[ -]*?){13,16}\b', '[REDACTED_CARD]', text)
    return text


# --- REQUEST MODELS ---
class SchemaRequest(BaseModel):
    catalog: str = Field(default="main", description="Target Unity Catalog name")
    schema_name: str = Field(default="default", description="Target Schema name")

class QueryRequest(BaseModel):
    sql_query: str = Field(..., description="SQL SELECT query to execute on Databricks")


# --- HEALTH CHECK ENDPOINT ---
@app.get("/healthz")
def health_check():
    return {"status": "HEALTHY", "connector": "databricks-gemini-bridge"}


# --- TOOL 1: DISCOVER SCHEMA ---
@app.post("/tools/get_schema", summary="Discover Unity Catalog Tables")
def get_schema(req: SchemaRequest, authorization: str = Header(...)):
    """Allows Gemini to discover available tables and columns in Databricks."""
    token = get_token(authorization)
    
    try:
        client = WorkspaceClient(host=workspace_host(), token=token)
        tables = client.tables.list(catalog_name=req.catalog, schema_name=req.schema_name)
        schema_info = []
        for t in tables:
            cols = [{"name": c.name, "type": c.type_text} for c in (t.columns or [])]
            schema_info.append({"table_name": t.name, "columns": cols})
            
        return {
            "status": "SUCCESS", 
            "catalog": req.catalog, 
            "schema": req.schema_name, 
            "tables": schema_info
        }
    except Exception as e:
        raise databricks_error(
            f"Unable to read catalog '{req.catalog}', schema '{req.schema_name}'",
            e,
        ) from e


# --- TOOL 2: EXECUTE SQL WITH GUARDRAILS ---
@app.post("/tools/execute_sql", summary="Execute SQL with PII Redaction")
def execute_sql(req: QueryRequest, authorization: str = Header(...)):
    """Executes a SQL query on Databricks SQL Warehouse and redacts PII."""
    token = get_token(authorization)
    
    # Security Rule: Block destructive SQL
    blocked_keywords = ["DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT"]
    if any(kw in req.sql_query.upper().split() for kw in blocked_keywords):
        return {
            "status": "BLOCKED",
            "reason": "Destructive query blocked. Only read-only SELECT operations are allowed."
        }

    try:
        if not SQL_WAREHOUSE_ID:
            raise RuntimeError("DATABRICKS_SQL_WAREHOUSE_ID is not configured.")
        client = WorkspaceClient(host=workspace_host(), token=token)
        response = client.statement_execution.execute_statement(
            statement=req.sql_query,
            warehouse_id=SQL_WAREHOUSE_ID
        )
        
        # Checked against StatementState instead of StatementExecutionStatus
        if response.status.state == StatementState.SUCCEEDED:
            raw_result = str(response.result.as_dict())
            cleaned_result = sanitize_pii(raw_result)
            return {"status": "SUCCESS", "data": cleaned_result}
        else:
            return {"status": "FAILED", "error": str(response.status.error)}
    except Exception as e:
        raise databricks_error("Unable to execute SQL", e) from e