# GCP_DB_Connector

Syncs data from Databricks into a Vertex AI Search (Discovery Engine) **Data Store**
in GCP, so a Gemini Enterprise app can be grounded on that data store and answer
natural-language questions.

## Architecture

```
Databricks SQL Warehouse (source of truth)
        │  databricks-sql-connector (poll, incremental or full)
        ▼
sync_databricks_to_datastore.py
        │  Discovery Engine Document Service (import/upsert)
        ▼
Vertex AI Search Data Store  ──►  Gemini Enterprise app (you attach this data store)
                                          │
                                          │  writes (custom Action, openapi_writeback.yaml)
                                          ▼
                                  writeback_server.py ──► Databricks (UPDATE)
```

Reads are grounded on the data store; writes go through a separate API because data
stores are read-only. A write sets the `last_updated` column, so the next sync pass
propagates it back into the data store.

There is no live query-time connector — rows are periodically upserted into the data
store, which Gemini Enterprise then searches/grounds on directly. Run the sync
frequently (loop mode, or a scheduled job every 1–5 min) to keep it near-real-time.

## Files

- **`create_datastore.py`** — one-time script that creates the Vertex AI Search Data
  Store in your GCP project (`DATASTORE_ID`), using the Discovery Engine SDK.
- **`sync_databricks_to_datastore.py`** — pulls rows from a Databricks table and
  upserts them into the data store as documents. Supports:
  - **Full refresh** (default) — pulls the whole table each run and reconciles the
    data store to match (`ReconciliationMode.FULL`), so deleted rows are removed too.
  - **Incremental sync** — set `DATABRICKS_UPDATED_COLUMN` to only pull rows changed
    since the last run (tracked in `.last_sync`), using `ReconciliationMode.INCREMENTAL`.
  - **Run mode** — `SYNC_MODE=once` for a single pass (e.g. triggered by Cloud
    Scheduler), or `SYNC_MODE=loop` to poll continuously every `SYNC_INTERVAL_SECONDS`.
- **`Dockerfile`** — container image for running the sync as a Cloud Run Job/service.
- **`writeback_server.py`** — FastAPI service the agent calls to update Databricks rows.
  Only whitelisted columns (`stock_count`, `price`, `category`, `product_name`) can be
  written, values are bound as query parameters, and requests require an `X-API-Key`
  header matching `WRITEBACK_API_KEY`.
- **`openapi_writeback.yaml`** — OpenAPI spec to import when registering the write-back
  API as a custom Action in Gemini Enterprise.
- **`Dockerfile.writeback`** — container image for the write-back service.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in Databricks + GCP config
gcloud auth application-default login   # or use a service account in production
```

Create the data store once:

```bash
python create_datastore.py
```

Run a sync:

```bash
python sync_databricks_to_datastore.py          # one pass
SYNC_MODE=loop python sync_databricks_to_datastore.py   # continuous near-real-time
```

## Deploy the sync job to GCP

Build and push the image, then run it as a Cloud Run Job on a schedule (recommended
for near-real-time without keeping a process running 24/7):

```bash
gcloud builds submit --tag gcr.io/<project-id>/databricks-datastore-sync

gcloud run jobs create databricks-datastore-sync \
  --image gcr.io/<project-id>/databricks-datastore-sync \
  --region <your-region> \
  --set-env-vars-file .env

gcloud scheduler jobs create http databricks-datastore-sync-trigger \
  --schedule="*/2 * * * *" \
  --uri="https://<region>-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/<project-id>/jobs/databricks-datastore-sync:run" \
  --http-method=POST \
  --oauth-service-account-email=<invoker-service-account>
```

(Or deploy as a Cloud Run **service** with `SYNC_MODE=loop` if you prefer a
continuously running sync instead of a scheduled job.)

## Attach to Gemini Enterprise

In the Gemini Enterprise / Agentspace console, create or edit your app and attach the
data store (`DATASTORE_ID`) as a data source. Once attached, the app can answer
natural-language questions grounded on the synced Databricks rows.

## Write-back

Run the write-back API locally:

```bash
uvicorn writeback_server:app --reload --port 8000
```

Test it:

```bash
curl -X POST http://localhost:8000/tools/update_product \
  -H "X-API-Key: $WRITEBACK_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "P101", "column": "stock_count", "value": "20"}'
```

Deploy it. `Dockerfile` is the sync image, so the write-back service is built from
`Dockerfile.writeback` via a Cloud Build config. Cloud Run's `--env-vars-file` expects
YAML, so generate one from `.env` first (it holds secrets and is gitignored):

```bash
python3 - <<'EOF'
import pathlib
keep = ["DATABRICKS_HOST","DATABRICKS_HTTP_PATH","DATABRICKS_TOKEN","DATABRICKS_CATALOG",
        "DATABRICKS_SCHEMA","DATABRICKS_TABLE","DATABRICKS_ID_COLUMN",
        "DATABRICKS_UPDATED_COLUMN","WRITEBACK_API_KEY"]
env = dict(
    line.split("=", 1)
    for line in (l.strip() for l in pathlib.Path(".env").read_text().splitlines())
    if line and not line.startswith("#") and "=" in line
)
pathlib.Path("env.writeback.yaml").write_text(
    "\n".join(f'{k}: "{env[k]}"' for k in keep if k in env) + "\n"
)
EOF

gcloud builds submit \
  --config cloudbuild.writeback.yaml \
  --substitutions _IMAGE=gcr.io/<project-id>/databricks-writeback

gcloud run deploy databricks-writeback \
  --image gcr.io/<project-id>/databricks-writeback \
  --region <your-region> \
  --env-vars-file env.writeback.yaml
```

Then in the Gemini Enterprise console, add a custom **Action** to your app and import
[openapi_writeback.yaml](openapi_writeback.yaml), setting `servers.url` to the deployed
Cloud Run URL and configuring the API key as the `X-API-Key` header value. The agent can
then handle requests like "set stock for P101 to 20", and the change appears in query
results after the next sync pass.

## Security notes

- Never hardcode secrets (tokens, project IDs with credentials) in source files — use
  environment variables / Secret Manager. Rotate any credential that was ever typed
  into a file, even if not committed.
- Scope the Databricks token to least privilege (read-only on the synced table).
- Use a dedicated GCP service account with only `roles/discoveryengine.editor` (or
  narrower) for the sync job, not broad project-owner credentials.
