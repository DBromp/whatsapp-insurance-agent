# whatsapp-insurance-agent

> Self-managing medallion data pipeline for WhatsApp auto-insurance sales conversations.

Built for the NMSTX Data & AI Engineering technical assessment. The agent creates, monitors, and self-heals a three-layer Delta Lake pipeline over ~153k WhatsApp messages between insurance leads and human sellers.

## Why this exists

The brief asks for an AI agent that builds and manages a transformation pipeline — not a one-off analysis script. This repo demonstrates the difference: persistent infrastructure (Databricks Workflows, Auto Loader, Delta MERGE patterns) wrapped by a supervising Python agent that monitors job runs, diagnoses failures via Gemini, and either auto-applies safe patches or escalates with structured alerts.

## Architecture

```
                    ┌────────────────┐
                    │  Volume        │  parquet files land here
                    │  (Unity Cat.)  │
                    └────────┬───────┘
                             │ Auto Loader
                             ▼
                    ┌────────────────┐
                    │  BRONZE        │  append-only, schema-enforced
                    │  bronze.messages│
                    └────────┬───────┘
                             │ PySpark transforms + Gemini extraction
                             ▼
                    ┌────────────────┐
                    │  SILVER        │  cleaned, PII-masked, enriched
                    │  silver.messages│
                    │  silver.conversations│
                    └────────┬───────┘
                             │ Aggregations + LLM classification
                             ▼
                    ┌────────────────┐
                    │  GOLD          │  8 analytical tables
                    │  (see below)   │
                    └────────────────┘
                             ▲
                             │ monitor + diagnose + patch
                    ┌────────┴───────┐
                    │  SUPERVISOR    │  custom Python control loop
                    │  agent/        │  (Gemini-powered)
                    └────────────────┘
```

## Pipeline layers

### Bronze
Raw WhatsApp messages from Auto Loader. Schema enforced. Append-only. Partitioned by ingest date.

### Silver
- `silver.messages` — parsed metadata, masked PII, deduped
- `silver.conversations` — conversation-level rollup with LLM-extracted vehicle, competitor, and objection signals
- `silver._pii_vault` — access-restricted token map

### Gold
Eight analytical tables refreshing incrementally from Silver:

1. `gold.agent_scorecard` — seller performance metrics
2. `gold.funnel_stages` — deepest sales stage reached per conversation
3. `gold.objection_taxonomy` — clustered rejection themes
4. `gold.competitor_matrix` — win/loss vs each competitor brand
5. `gold.ghosting_predictors` — features correlating with lead dropoff
6. `gold.vehicle_cohorts` — close rate by vehicle segment
7. `gold.lead_intent_score` — composite buying-intent index
8. `gold.conversation_quality` — coaching score per conversation

## The supervising agent

Located in `agent/supervisor.py`. Built incrementally over the four days, fully wired on Day 4.

Control loop:
1. Poll Databricks Jobs API for recent runs
2. On failure: capture stderr, notebook source, last N log lines
3. Send to Gemini with a diagnosis prompt → structured patch proposal
4. Classify patch as safe-auto-apply (schema additions, null-handling) vs human-required (logic changes)
5. Auto-apply on safe patches; otherwise emit structured alert to `_pipeline_alerts`

## Quickstart

```bash
# Clone
git clone https://github.com/DBromp/whatsapp-insurance-agent.git
cd whatsapp-insurance-agent

# Install dev dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run tests
pytest

# Deploy to Databricks
# See docs/deploy.md for catalog/Volume/secrets setup
```

## Documentation

- `docs/decisions.md` — Architecture Decision Records (ADRs)
- `docs/deploy.md` — Databricks setup walkthrough
- `docs/runbook.md` — Operational runbook (added on Day 4)

## Status

| Day | Layer | Status |
|---|---|---|
| 1 | Foundation + Bronze | In progress |
| 2 | Silver | Pending |
| 3 | Gold + incremental | Pending |
| 4 | Agent + ship | Pending |

## License

MIT — see [LICENSE](LICENSE).
