# Databricks ↔ Gemini Enterprise Connector — Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    GEMINI ENTERPRISE APP                         │
│                  (Gemini for Google Workspace)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Custom Action (HTTPS + X-API-Key)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATABRICKS CONNECTOR  (Cloud Run)                   │
│                    unified_server.py                             │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ /tools/         │  │ /tools/          │  │ /tools/       │  │
│  │ get_schema      │  │ execute_sql      │  │ update_product│  │
│  │                 │  │                  │  │               │  │
│  │ Discover tables │  │ Run SELECT query │  │ UPDATE row in │  │
│  │ & columns       │  │ with PII redact  │  │ Databricks    │  │
│  └────────┬────────┘  └────────┬─────────┘  └──────┬────────┘  │
│           │                    │                    │           │
│           └────────────────────┴────────────────────┘           │
│                                │  Databricks SQL Connector       │
│                                ▼                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           BACKGROUND SYNC THREAD (every 60s)            │    │
│  │                                                         │    │
│  │   Databricks ──► GCS Bucket ──► Vertex AI Search        │    │
│  │   (products)     (NDJSON)       (indexed for search)    │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────┬──────────────────────────┬───────────────────────────┘
           │                          │
           ▼                          ▼
┌─────────────────┐       ┌───────────────────────┐
│   DATABRICKS    │       │   VERTEX AI SEARCH     │
│                 │       │   (Discovery Engine)   │
│ hackathon_db    │       │                        │
│ .inventory_     │       │ databricks-inventory-  │
│  system.products│       │ datastore-gemini       │
│                 │       │                        │
│ product_id      │       │ Indexed rows for       │
│ product_name    │       │ semantic search by     │
│ category        │       │ Gemini                 │
│ stock_count     │       └───────────────────────┘
│ price           │                  ▲
│ last_updated    │──────────────────┘
└─────────────────┘   GCS: databricks-connector-sync
                           sync/products.ndjson
```

---

## The Two Flows

### Read / Write — Real-time (direct Databricks)

```
User prompt in Gemini:  "Update Mechanical Keyboard stock to 20"
  → Gemini calls  POST /tools/update_product  { product_id, column, value }
  → Connector runs  SQL UPDATE  on Databricks
  → Returns updated row instantly
```

```
User prompt in Gemini:  "Show me all Electronics products under $200"
  → Gemini calls  POST /tools/execute_sql  { sql_query }
  → Connector runs  SELECT query  on Databricks
  → Returns results with PII auto-redacted
```

### Search / Discovery — Near real-time (via Vertex AI Search)

```
Background thread (every 60s):
  Databricks products table
    → exported as NDJSON
    → uploaded to  gs://databricks-connector-sync/sync/products.ndjson
    → imported into  Vertex AI Search datastore (databricks-inventory-datastore-gemini)

Gemini searches the indexed datastore for semantic queries.
```

---

## Key Components

| Component | Value |
|---|---|
| GCP Project | `hl2-gcpp-ccoe-ge-h-data-1713-t` |
| Cloud Run Service | `databricks-connector` (us-central1) |
| Databricks Catalog | `hackathon_db.inventory_system.products` |
| GCS Bucket | `gs://databricks-connector-sync` |
| Vertex AI Search Datastore | `databricks-inventory-datastore-gemini` |
| Sync Interval | 60 seconds |
| Auth (Gemini → Connector) | `X-API-Key` header |

## API Endpoints

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /healthz` | None | Health check |
| `GET /sync/status` | None | Check sync state |
| `POST /tools/get_schema` | X-API-Key | Discover Databricks tables & columns |
| `POST /tools/execute_sql` | X-API-Key | Run read-only SQL queries |
| `POST /tools/update_product` | X-API-Key | Write changes back to Databricks |
