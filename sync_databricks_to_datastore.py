"""Syncs rows from a Databricks table into a Vertex AI Search Data Store.

Each row becomes one document (upserted by DATABRICKS_ID_COLUMN). If
DATABRICKS_UPDATED_COLUMN is set, only rows changed since the last run are
pulled (incremental reconciliation); otherwise the whole table is pulled and
reconciled as a full refresh, so deleted rows are removed from the data store too.

Usage:
    python sync_databricks_to_datastore.py            # single pass
    SYNC_MODE=loop python sync_databricks_to_datastore.py  # continuous, near-real-time
"""
import json
import os
import time
from pathlib import Path

from databricks import sql as databricks_sql
from dotenv import load_dotenv
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine

load_dotenv()

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_CATALOG = os.environ["DATABRICKS_CATALOG"]
DATABRICKS_SCHEMA = os.environ["DATABRICKS_SCHEMA"]
DATABRICKS_TABLE = os.environ["DATABRICKS_TABLE"]
DATABRICKS_ID_COLUMN = os.environ.get("DATABRICKS_ID_COLUMN", "id")
DATABRICKS_UPDATED_COLUMN = os.environ.get("DATABRICKS_UPDATED_COLUMN", "")

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_LOCATION = os.environ.get("GCP_LOCATION", "global")
DATASTORE_ID = os.environ["DATASTORE_ID"]

SYNC_MODE = os.environ.get("SYNC_MODE", "once")  # "once" or "loop"
SYNC_INTERVAL_SECONDS = int(os.environ.get("SYNC_INTERVAL_SECONDS", "60"))
STATE_FILE = Path(os.environ.get("SYNC_STATE_FILE", ".last_sync"))
IMPORT_BATCH_SIZE = 100


def fetch_rows(since: str | None) -> list[dict]:
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE}"
    query = f"SELECT * FROM {table}"
    if since and DATABRICKS_UPDATED_COLUMN:
        query += f" WHERE {DATABRICKS_UPDATED_COLUMN} > '{since}'"

    # First query after the warehouse has auto-suspended can take a few minutes to start.
    print(f"Connecting to Databricks warehouse at {DATABRICKS_HOST}...", flush=True)
    with databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def rows_to_documents(rows: list[dict]) -> list[discoveryengine.Document]:
    documents = []
    for row in rows:
        doc_id = str(row[DATABRICKS_ID_COLUMN])
        documents.append(
            discoveryengine.Document(
                id=doc_id,
                json_data=json.dumps(row, default=str),
            )
        )
    return documents


def import_documents(documents: list[discoveryengine.Document], full_refresh: bool) -> None:
    client_options = (
        ClientOptions(api_endpoint=f"{GCP_LOCATION}-discoveryengine.googleapis.com")
        if GCP_LOCATION != "global"
        else None
    )
    client = discoveryengine.DocumentServiceClient(client_options=client_options)
    parent = client.branch_path(
        project=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        data_store=DATASTORE_ID,
        branch="default_branch",
    )

    # ImportDocuments only supports FULL reconciliation for GCS/BigQuery sources, not
    # inline documents. Emulate a full refresh by purging stale docs first, then
    # upserting the current rows with INCREMENTAL.
    if full_refresh:
        purge_operation = client.purge_documents(
            request=discoveryengine.PurgeDocumentsRequest(parent=parent, filter="*")
        )
        purge_operation.result()
        print("Purged existing documents for full refresh.", flush=True)

    if not documents:
        print("No rows to sync.", flush=True)
        return

    for i in range(0, len(documents), IMPORT_BATCH_SIZE):
        batch = documents[i : i + IMPORT_BATCH_SIZE]
        operation = client.import_documents(
            request=discoveryengine.ImportDocumentsRequest(
                parent=parent,
                inline_source=discoveryengine.ImportDocumentsRequest.InlineSource(
                    documents=batch
                ),
                reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
            )
        )
        operation.result()
        print(f"Synced {len(batch)} rows ({i + len(batch)}/{len(documents)}).", flush=True)


def run_once() -> None:
    since = STATE_FILE.read_text().strip() if STATE_FILE.exists() else None
    incremental = bool(since and DATABRICKS_UPDATED_COLUMN)

    rows = fetch_rows(since)
    documents = rows_to_documents(rows)
    import_documents(documents, full_refresh=not incremental)

    if DATABRICKS_UPDATED_COLUMN and rows:
        latest = max(str(row[DATABRICKS_UPDATED_COLUMN]) for row in rows)
        STATE_FILE.write_text(latest)


def main() -> None:
    if SYNC_MODE == "loop":
        while True:
            run_once()
            time.sleep(SYNC_INTERVAL_SECONDS)
    else:
        run_once()


if __name__ == "__main__":
    main()
