================================================================================
  GCP + DATABRICKS CONNECTOR — HIGH-LEVEL ARCHITECTURE
  Project: hl2-gcpp-ccoe-ge-h-data-1713-t | Region: us-central1
================================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│                       GEMINI ENTERPRISE AGENTSPACE                          │
│                                                                             │
│   User Query ──► Agent (LLM)                                               │
│                      │                                                     │
│          ┌───────────┴────────────┐                                        │
│          │ READ PATH              │ WRITE PATH                             │
│          ▼                        ▼                                        │
│   Vertex AI Search           MCP Server Tool                               │
│   (RAG / Semantic Search)    (Custom Tool registered in Agentspace)        │
└──────────┼────────────────────────┼───────────────────────────────────────┘
           │                        │ MCP Protocol over HTTP/SSE
           │                        │   GET  /mcp/sse
           │                        │   POST /mcp/messages/
           │               ┌────────▼──────────────────────────────────────┐
           │               │   CLOUD RUN — databricks-mcp-server           │
           │               │   Image: Dockerfile.server                    │
           │               │   URL: databricks-mcp-server-                 │
           │               │        204408153990.us-central1.run.app       │
           │               │                                               │
           │               │  ┌──────────────────────────────────────────┐ │
           │               │  │   FastAPI App  (unified_server.py)       │ │
           │               │  │                                          │ │
           │               │  │  MCP Endpoints (Agentspace connects here)│ │
           │               │  │  ├── GET  /mcp/sse                       │ │
           │               │  │  └── POST /mcp/messages/                 │ │
           │               │  │         ├── insert_product() ──┐         │ │
           │               │  │         ├── update_product() ──┤         │ │
           │               │  │         ├── execute_sql()       │         │ │
           │               │  │         └── get_schema()        │         │ │
           │               │  │                                 │         │ │
           │               │  │  REST Endpoints (X-API-Key auth)│         │ │
           │               │  │  ├── POST /tools/insert_product  │         │ │
           │               │  │  ├── POST /tools/update_product  │         │ │
           │               │  │  ├── POST /tools/execute_sql     │         │ │
           │               │  │  ├── POST /tools/get_schema      │         │ │
           │               │  │  └── GET  /sync/status           │         │ │
           │               │  │                                 │         │ │
           │               │  │  Background Sync Thread          │         │ │
           │               │  │  _sync_trigger.set() ◄───────────┘         │ │
           │               │  │  (wakes immediately on write)             │ │
           │               │  │  (also runs every 60s as safety net)      │ │
           │               │  └──────────────────┬───────────────────────┘ │
           │               └────────────────────┼──────────────────────────┘
           │                                    │ SQL (Databricks connector)
           │                        ┌───────────▼─────────────┐
           │                        │       DATABRICKS         │
           │                        │   Unity Catalog          │
           │                        │   hackathon_db           │
           │                        │     .inventory_system    │
           │                        │       .products          │
           │                        │                          │
           │                        │   SQL Warehouse          │
           │                        │   aa7b310aa23458d2       │
           │                        │   Host: dbc-c486d425     │
           │                        │   -ef89.cloud            │
           │                        │   .databricks.com        │
           │                        └───────────┬─────────────┘
           │                                    │ Export as NDJSON
           │                        ┌───────────▼─────────────┐
           │                        │      GCS BUCKET          │
           │                        │  databricks-connector    │
           │                        │  -sync                   │
           │                        │  /sync/products.ndjson   │
           │                        └───────────┬─────────────┘
           │                                    │ Import documents
           │                        ┌───────────▼─────────────┐
           └───────────────────────►│   VERTEX AI SEARCH       │
                                    │   (Discovery Engine)     │
                                    │   databricks-inventory   │
                                    │   -datastore-gemini      │
                                    └─────────────────────────┘


================================================================================
  DATA FLOWS
================================================================================

READ PATH
─────────
  1. User asks a question in Gemini Enterprise Agentspace
  2. Agentspace queries Vertex AI Search (semantic / RAG)
  3. Vertex AI Search returns matching product documents
  4. Agent synthesises the answer and responds to user

WRITE PATH (immediate sync)
────────────────────────────
  1. User asks to insert or update a product in Agentspace
  2. Agentspace calls the MCP Server via SSE (GET /mcp/sse)
  3. MCP Server negotiates tools/list → tools/call
  4. insert_product / update_product writes row to Databricks
  5. _sync_trigger.set() wakes the background sync thread instantly
  6. Sync thread exports updated rows → GCS (NDJSON)
  7. Sync thread imports GCS file → Vertex AI Search
  8. Record is searchable within seconds of the write

PERIODIC SYNC (safety net)
───────────────────────────
  - Background thread also runs every 60 seconds (SYNC_INTERVAL_SECONDS)
  - Catches any changes made directly in Databricks outside the app
  - Uses .last_sync state file for incremental (changed rows only) sync


================================================================================
  COMPONENT REFERENCE
================================================================================

  Component                  Detail
  ─────────────────────────  ────────────────────────────────────────────────
  GCP Project                hl2-gcpp-ccoe-ge-h-data-1713-t
  Cloud Run Service          databricks-mcp-server  (us-central1)
  Cloud Run Image            Dockerfile.server → python:3.11-slim + FastAPI
  Vertex AI Search           databricks-inventory-datastore-gemini (global)
  GCS Staging Bucket         databricks-connector-sync
  Databricks Host            dbc-c486d425-ef89.cloud.databricks.com
  Databricks Catalog         hackathon_db
  Databricks Schema          inventory_system
  Databricks Table           products
  Databricks Warehouse       aa7b310aa23458d2
  MCP SSE URL                /mcp/sse
  MCP Messages URL           /mcp/messages/
  Auth (REST endpoints)      X-API-Key header (WRITEBACK_API_KEY env var)
  Auth (Cloud Run)           roles/run.invoker (EPAM org — no allUsers)


================================================================================
  FILE STRUCTURE
================================================================================

  GCP_DB_Connector/
  ├── unified_server.py           FastAPI app: MCP server + REST API + sync
  ├── sync_databricks_to_datastore.py  Databricks → GCS → Vertex AI Search
  ├── create_datastore.py         Idempotent Vertex AI Search data store setup
  ├── Dockerfile.server           Builds the Cloud Run web server image
  ├── Dockerfile                  Builds the standalone sync job image
  ├── requirements.txt            Python dependencies
  ├── .env                        Local credentials (not committed)
  └── docs/
      └── architecture.txt        This file


================================================================================
