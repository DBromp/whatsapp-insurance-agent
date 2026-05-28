---
name: whatsapp-insurance-pipeline
description: Project context for the NMSTX Data & AI Engineering technical test — building an agentic medallion data pipeline (Bronze to Silver to Gold) over WhatsApp auto-insurance conversations on Databricks Free Edition with Google Gemini for extraction and self-healing. Use whenever working on this project — building or debugging pipeline transforms, designing the agent control loop, planning Gold-layer insights, masking Brazilian PII, or interpreting the Bronze schema. Pin this as the source of truth for project decisions, terminology, and known data quirks.
---

# WhatsApp insurance pipeline — project context

## What this project is

A 4-day technical assessment for NMSTX. The goal is to build an AI agent that creates, manages, and maintains a 3-layer medallion data pipeline over a 153k-message WhatsApp auto-insurance sales dataset. Not a one-off analysis — persistent infrastructure that self-heals and auto-refreshes Gold as new Bronze data arrives.

See `references/overview.md` for the full brief and evaluation criteria.

## Tech stack (locked)

- Storage and compute: Databricks Free Edition (serverless, Unity Catalog, Delta Lake)
- Catalog: `nmstx_whatsapp_pipeline` with schemas `bronze`, `silver`, `gold`
- Language: Pure Python (PySpark for transforms, pure Python for the agent layer)
- LLM: Google Gemini 2.5 Flash (free tier, ~15 RPM, ~1500 RPD)
- Repo: new public GitHub (name TBD)
- Orchestration: Databricks Workflows + a Python control-loop agent

## Critical project rules

1. The agent must build the pipeline — not perform analysis. This is the explicit make-or-break criterion. The agent's job is creating, monitoring, and repairing transformation jobs that run on a schedule.
2. Live Gold layer. When new rows land in Bronze, Gold must update automatically (Auto Loader / DLT / scheduled incremental).
3. Self-healing. On failure, the agent captures the traceback, diagnoses via Gemini, and either auto-applies safe patches or escalates with a structured alert.
4. Mask all PII preserving dimensions. Names use a stable token. CPF, CEP, phone, email, plate become format-preserving tokens of identical shape.
5. Avoid the brief's example insights. Email-provider stats, basic personas, vanilla sentiment — these are explicitly discouraged. Use the 8 Gold insights in `references/gold_insights.md` instead.

## Knowledge base navigation

- `references/overview.md` — full brief, requirements, evaluation criteria
- `references/data_profile.md` — actual data shape from profiling (real distributions, deltas from the data dictionary)
- `references/schema.md` — Bronze schema, all 14 columns + JSON metadata fields
- `references/entities.md` — lead, agent, conversation, campaign definitions and identity rules
- `references/transformations.md` — Bronze to Silver to Gold transformation specs
- `references/gold_insights.md` — the 8 Gold-layer outputs we're building
- `references/pii_masking.md` — Brazilian PII patterns and dimension-preserving masking strategy
- `references/glossary.md` — PT-BR insurance terminology
- `references/databricks_setup.md` — catalog/schema/volume layout and Free Edition gotchas
