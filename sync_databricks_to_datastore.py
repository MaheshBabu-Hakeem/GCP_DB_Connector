"""Syncs rows from a Databricks table → GCS (NDJSON) → Vertex AI Search Data Store.

Flow each sync pass:
  1. Pull rows from Databricks (full table or incremental if DATABRICKS_UPDATED_COLUMN is set).
  2. Write rows as NDJSON to gs://GCS_BUCKET_NAME/sync/<table>.ndjson.
  3. Import that GCS file into the Vertex AI Search data store.

run_once() is called by unified_server.py on a background thread; it can also run standalone:
    python sync_databricks_to_datastore.py
"""
import json
import os
import time
from pathlib import Path

from databricks import sql as databricks_sql
from dotenv import load_dotenv
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
from google.cloud import storage as gcs

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
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]

SYNC_MODE = os.environ.get("SYNC_MODE", "once")
SYNC_INTERVAL_SECONDS = int(os.environ.get("SYNC_INTERVAL_SECONDS", "60"))
STATE_FILE = Path(os.environ.get("SYNC_STATE_FILE", ".last_sync"))


def ensure_gcs_bucket() -> None:
    """Create the GCS bucket if it does not exist."""
    client = gcs.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET_NAME)
    if not bucket.exists():
        location = GCP_LOCATION if GCP_LOCATION != "global" else "US"
        bucket.create(location=location)
        print(f"Created GCS bucket: gs://{GCS_BUCKET_NAME}", flush=True)
    else:
        print(f"GCS bucket gs://{GCS_BUCKET_NAME} ready.", flush=True)


def fetch_rows(since: str | None) -> list[dict]:
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE}"
    query = f"SELECT * FROM {table}"
    if since and DATABRICKS_UPDATED_COLUMN:
        query += f" WHERE {DATABRICKS_UPDATED_COLUMN} > '{since}'"

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


def export_to_gcs(rows: list[dict]) -> str:
    """Write rows as NDJSON to GCS. Returns the gs:// URI."""
    blob_path = f"sync/{DATABRICKS_TABLE}.ndjson"
    blob = gcs.Client(project=GCP_PROJECT_ID).bucket(GCS_BUCKET_NAME).blob(blob_path)
    ndjson = "\n".join(
        json.dumps({"id": str(row[DATABRICKS_ID_COLUMN]), "jsonData": json.dumps(row, default=str)})
        for row in rows
    )
    blob.upload_from_string(ndjson, content_type="application/json")
    uri = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
    print(f"Exported {len(rows)} rows to {uri}", flush=True)
    return uri


def import_documents(rows: list[dict], full_refresh: bool) -> None:
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

    if full_refresh:
        op = client.purge_documents(
            request=discoveryengine.PurgeDocumentsRequest(parent=parent, filter="*")
        )
        op.result()
        print("Purged existing documents for full refresh.", flush=True)

    if not rows:
        print("No rows to sync.", flush=True)
        return

    gcs_uri = export_to_gcs(rows)
    op = client.import_documents(
        request=discoveryengine.ImportDocumentsRequest(
            parent=parent,
            gcs_source=discoveryengine.GcsSource(
                input_uris=[gcs_uri],
                data_schema="document",
            ),
            reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
        )
    )
    op.result()
    print(f"Imported {len(rows)} rows from GCS into data store.", flush=True)


def run_once() -> int:
    """Run one sync pass. Returns the number of rows synced."""
    since = STATE_FILE.read_text().strip() if STATE_FILE.exists() else None
    incremental = bool(since and DATABRICKS_UPDATED_COLUMN)

    rows = fetch_rows(since)
    import_documents(rows, full_refresh=not incremental)

    if DATABRICKS_UPDATED_COLUMN and rows:
        latest = max(str(row[DATABRICKS_UPDATED_COLUMN]) for row in rows)
        STATE_FILE.write_text(latest)

    print(f"Sync complete: {len(rows)} rows.", flush=True)
    return len(rows)


def main() -> None:
    if SYNC_MODE == "loop":
        while True:
            run_once()
            time.sleep(SYNC_INTERVAL_SECONDS)
    else:
        run_once()


if __name__ == "__main__":
    main()
