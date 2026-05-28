# Architecture Decision Log

Non-obvious design tradeoffs captured during the 4-day build. Each ADR follows the format: context, decision, consequences.

---

## ADR-001 — Databricks Free Edition as the platform

**Date:** Day 0
**Status:** Accepted

**Context.** The brief offers explicit bonus credit for using Databricks (free tier acceptable). We considered local Python + DuckDB and cloud (AWS/GCP) alternatives. Local would have been fastest but loses the bonus credit; cloud adds setup time without commensurate signal.

**Decision.** Use Databricks Free Edition. Catalog `nmstx_whatsapp_pipeline`, schemas `bronze`, `silver`, `gold`. Raw files land in a Unity Catalog Volume.

**Consequences.**
- Serverless-only compute — no all-purpose clusters in Free Edition.
- Monthly compute hours capped. We aggressively cache LLM extractions to Silver so reruns are cheap.
- We can demonstrate Auto Loader, Unity Catalog, Volumes, Delta Lake, and Workflows — every "killer" Databricks-idiomatic feature.

---

## ADR-002 — Google Gemini 2.5 Flash as the LLM

**Date:** Day 0
**Status:** Accepted

**Context.** We need an LLM for unstructured text extraction (vehicle data, competitors, objections, claim history) over ~153k messages, plus reasoning for the supervising agent. The LLM must be free-or-near-free, support structured output, handle PT-BR well, and have generous-enough rate limits to extract thousands of conversations per day.

**Decision.** Google Gemini 2.5 Flash via the `google-genai` SDK. Free-tier quotas (15 RPM, ~1500 RPD) are sufficient when paired with batched calls (~50 items per request) and persistent caching of extraction results.

**Consequences.**
- We carry one external dependency (`google-genai`).
- Rate limits drive batching architecture — we accept the latency to stay within the free tier.
- Fallback: Groq Llama-3.3-70B free tier if Gemini quotas tighten.

---

## ADR-003 — Plain PySpark, not Delta Live Tables

**Date:** Day 0
**Status:** Accepted

**Context.** DLT offers declarative tables, built-in expectations, and a lineage UI. Plain PySpark offers maximum control, simpler debugging, and a uniform failure model the agent can introspect.

**Decision.** Use plain PySpark + Workflows for both Silver and Gold. Keep DLT in reserve.

**Consequences.**
- The supervising agent only has to understand JobRun failures, not DLT pipeline events — substantially simpler.
- LLM batched extraction is natural (we control batch size, retries, rate limits explicitly).
- We give up DLT's auto-incremental and lineage UI, but we still get strong Databricks-idiomatic signal via Auto Loader, Unity Catalog, Delta MERGE patterns, and Workflows.

---

## ADR-004 — Custom Python control loop for the supervising agent

**Date:** Day 0
**Status:** Accepted

**Context.** Choice between LangGraph (standard agentic framework) and a custom Python control loop.

**Decision.** Custom Python loop. The agent's flow — poll → diagnose → patch or escalate — is essentially linear with retries. A graph DSL adds abstraction without solving anything we don't already need to solve.

**Consequences.**
- Smaller dependency surface.
- Full control over retry, state, and idempotency semantics.
- We document the state machine explicitly in `agent/supervisor.py` instead of inferring it from a graph definition.

---

## ADR-005 — Strict Bronze schema enforcement, fail-fast on drift

**Date:** Day 1
**Status:** Accepted

**Context.** Bronze receives raw parquet from an upstream source we don't control. The brief asks for self-healing — but auto-applying schema changes silently in Bronze would defeat the agent's purpose (no failure means nothing to heal).

**Decision.** Bronze uses a static schema. Any drift fails the run and writes an audit record. The supervising agent decides whether the drift is a safe addition (auto-patch the schema) or a logic change (escalate).

**Consequences.**
- Bronze is brittle by design. That brittleness is what makes the agent's value visible.
- Auto Loader's `schemaEvolutionMode = "rescue"` captures drifted columns into `_rescued_data` for forensic visibility.

---
