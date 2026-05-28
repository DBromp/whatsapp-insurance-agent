# Databricks Free Edition setup

## Catalog and schema layout

```
nmstx_whatsapp_pipeline (catalog)
├── bronze (schema)
│   └── messages (Delta, append-only)
├── silver (schema)
│   ├── messages
│   ├── conversations
│   └── _pii_vault (access-restricted)
└── gold (schema)
    ├── agent_scorecard
    ├── funnel_stages
    ├── objection_taxonomy
    ├── competitor_matrix
    ├── ghosting_predictors
    ├── vehicle_cohorts
    ├── lead_intent_score
    └── conversation_quality
```

## Volumes

Raw source files land in a Unity Catalog Volume:

```
nmstx_whatsapp_pipeline.bronze.raw_files
└── conversations/
    ├── conversations_bronze.parquet (initial)
    └── *.parquet (future increments)
```

## Free Edition specifics

- Serverless-only compute — no all-purpose clusters in Free Edition. Use serverless notebooks and serverless Jobs.
- Monthly compute hours capped — budget runs. Avoid running the full LLM extraction on every dev iteration; cache extracted columns to Silver and only re-extract on new data.
- Workflows are available — use them for the scheduled Bronze -> Silver -> Gold pipeline (default cadence: every 15 min).
- Secrets API — store the Gemini API key in `nmstx-secrets/gemini-api-key`. Never inline in notebooks.
- No public networking restrictions on outbound — Gemini API calls work fine from serverless.
- Auto Loader is available — use for Bronze ingestion from the Volume.
- Delta Live Tables is available — consider for Silver if the declarative model fits, otherwise plain Spark jobs.

## Secrets

| Secret name | Purpose |
|---|---|
| `nmstx-secrets/gemini-api-key` | Google Gemini API key (free-tier) |
| `nmstx-secrets/pii-salt` | Salt for deterministic PII tokenization |

## Job schedule (proposed)

| Job | Trigger | Notebook |
|---|---|---|
| `bronze_ingest` | Auto Loader trigger on Volume | `01_bronze_ingest.py` |
| `silver_transform` | Every 15 min | `02_silver_transform.py` |
| `gold_refresh` | After silver_transform succeeds | `03_gold_refresh.py` |
| `agent_supervisor` | Continuous (long-running) | `04_agent_supervisor.py` |
