# Databricks → Gemini Enterprise — Read Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    GEMINI ENTERPRISE APP                         │
│                  (Gemini for Google Workspace)                   │
│                                                                  │
│   User: "Show me all Electronics products under $200"           │
│   User: "What columns does the products table have?"            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           │  HTTPS POST  +  X-API-Key header
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATABRICKS CONNECTOR  (Cloud Run)                   │
│                                                                  │
│         ┌─────────────────────┐   ┌─────────────────────┐      │
│         │  /tools/get_schema  │   │  /tools/execute_sql  │      │
│         │                     │   │                      │      │
│         │  Lists all tables   │   │  Runs SELECT query   │      │
│         │  and their columns  │   │  Auto-redacts PII    │      │
│         │  from Unity Catalog │   │  Blocks DROP/DELETE  │      │
│         └──────────┬──────────┘   └──────────┬───────────┘      │
│                    │                          │                  │
└────────────────────┼──────────────────────────┼──────────────────┘
                     │   Databricks SQL          │
                     │   Connector (JDBC)        │
                     ▼                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DATABRICKS                                │
│                                                                  │
│   Catalog : hackathon_db                                         │
│   Schema  : inventory_system                                     │
│   Table   : products                                             │
│                                                                  │
│   ┌────────────┬────────────────────┬──────────┬───────┐        │
│   │ product_id │ product_name       │ category │ price │  ...   │
│   ├────────────┼────────────────────┼──────────┼───────┤        │
│   │ P101       │ Wireless Mouse     │Electronics│ 29.99│        │
│   │ P102       │ Mechanical Keyboard│Electronics│129.50│        │
│   │ P103       │ 4K Monitor         │Electronics│399.00│        │
│   │ P104       │ Standing Desk      │Furniture  │499.99│        │
│   └────────────┴────────────────────┴──────────┴───────┘        │
└─────────────────────────────────────────────────────────────────┘
                     │
                     │  Results returned to Gemini
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GEMINI ENTERPRISE APP                         │
│                                                                  │
│   Gemini: "Here are Electronics products under $200:            │
│            - Wireless Mouse     $29.99                          │
│            - Mechanical Keyboard $129.50"                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Read Flow — Step by Step

```
1. User asks a question in Gemini Enterprise App

2. Gemini identifies the right tool to call:
      GET_SCHEMA  →  to discover table structure
      EXECUTE_SQL →  to fetch actual data

3. Connector receives the request, validates X-API-Key

4. Connector connects to Databricks SQL Warehouse
      Host : dbc-c486d425-ef89.cloud.databricks.com
      Table: hackathon_db.inventory_system.products

5. Runs the SQL query (SELECT only — writes are blocked)

6. Auto-redacts PII (emails, SSNs, card numbers)

7. Returns clean results back to Gemini

8. Gemini formats the answer for the user
```

---

## Safety Rules (Read Mode)

| Rule | Detail |
|---|---|
| Allowed | `SELECT` queries only |
| Blocked | `DROP`, `DELETE`, `TRUNCATE`, `ALTER`, `UPDATE`, `INSERT` |
| PII Redaction | Emails → `[REDACTED_EMAIL]`, SSNs → `[REDACTED_SSN]` |
| Auth | Every request requires `X-API-Key` header |
