# Bronze to Silver to Gold transformation specs

## Bronze

Raw `.parquet` ingest into Delta — append-only, no transformations beyond schema enforcement.

- Table: `nmstx_whatsapp_pipeline.bronze.messages`
- Ingestion: Auto Loader from `Volume:/raw/conversations/*.parquet`
- Partitioning: by `_ingest_date` (added on write)
- Schema: matches `references/schema.md` byte-for-byte; metadata kept as raw JSON string

## Silver

Cleaned, normalized, PII-masked, with extracted entities. Three tables (two user-facing + one cache).

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
- LLM-extracted (via cache, see below): `vehicle_brand`, `vehicle_model`, `vehicle_year`, `competitors_mentioned[]`, `had_prior_sinistro` (bool), `objection_category` (price / coverage / trust / timing / none)
- `state`, `city`, `lead_source`, `device`

### `silver._extraction_cache` (hash-keyed LLM result cache)

Schema: `(conversation_id, body_hash, prompt_version, extracted_json, extracted_at)`

How it works on every Silver run:
1. For each conversation in scope, compute `body_hash = md5(concat(sorted(message_bodies)))`
2. Look up `(conversation_id, body_hash, prompt_version)` in the cache
3. If hit → use cached `extracted_json`, skip Gemini call
4. If miss → call Gemini, write the new row to the cache, use the fresh result

Why this matters:
- Free-tier Gemini limits (~1500 RPD) would blow up on naïve re-runs
- Pattern mirrors NMSTX's own `automagik-hive` Smart CSV RAG (cited ~450× faster reloads, ~99% cost savings)
- `prompt_version` in the key means changing the extraction prompt triggers a deliberate full re-extract

LLM extraction batched ~50 conversations per Gemini call to stay under rate limits.

## Gold

Analytical layer — eight tables, one per insight (see `gold_insights.md`). All Gold tables refresh incrementally via a scheduled Workflow that watches Silver's `_silver_updated_at` watermark.

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
