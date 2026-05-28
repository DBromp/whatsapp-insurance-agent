# Bronze to Silver to Gold transformation specs

## Bronze

Raw `.parquet` ingest into Delta — append-only, no transformations beyond schema enforcement.

- Table: `nmstx_whatsapp_pipeline.bronze.messages`
- Ingestion: Auto Loader from `Volume:/raw/conversations/*.parquet`
- Partitioning: by `_ingest_date` (added on write)
- Schema: matches `references/schema.md` byte-for-byte; metadata kept as raw JSON string

## Silver

Cleaned, normalized, PII-masked, with extracted entities. Two tables.

### `silver.messages` (one row per message)

Transformations applied:
1. Parse `timestamp` to TIMESTAMP
2. Explode `metadata` JSON into typed columns
3. Mask PII in `message_body` and `sender_phone`/`sender_name` (see `pii_masking.md`)
4. Dedupe by `message_id` (idempotent on Auto Loader re-runs)
5. Filter `channel != 'whatsapp'` if any appear
6. Add derived columns: `is_lead`, `body_length`, `has_pii_token`, `extracted_*` fields

### `silver.conversations` (one row per conversation)

Rollup from `silver.messages`:
- `conversation_id`, `agent_id`, `campaign_id`, `lead_phone_masked`, `lead_name_token`
- `outcome` (deterministic — same across all messages)
- `n_messages`, `n_inbound`, `n_outbound`, `first_ts`, `last_ts`, `duration_minutes`
- `length_bucket` (cold / short / medium / long per dictionary buckets)
- LLM-extracted: `vehicle_brand`, `vehicle_model`, `vehicle_year`, `competitors_mentioned[]`, `had_prior_sinistro` (bool), `objection_category` (price / coverage / trust / timing / none)
- `state`, `city`, `lead_source`, `device`

LLM extraction batched ~50 conversations per Gemini call to stay under rate limits.

## Gold

Analytical layer — eight tables, one per insight (see `gold_insights.md`). All Gold tables refresh incrementally via Delta Live Tables or a scheduled Workflow that watches Silver's `_silver_updated_at` watermark.

- `gold.agent_scorecard`
- `gold.funnel_stages`
- `gold.objection_taxonomy`
- `gold.competitor_matrix`
- `gold.ghosting_predictors`
- `gold.vehicle_cohorts`
- `gold.lead_intent_score`
- `gold.conversation_quality`

## Idempotency rules

- Every Silver/Gold write is a MERGE on a deterministic primary key — never an unguarded append
- The agent's self-healing patches MUST preserve this invariant or it triggers a rollback
