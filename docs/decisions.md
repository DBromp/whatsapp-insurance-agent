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
- Monthly compute hours capped. We aggressively cache LLM extractions to Silver so reruns are cheap (see ADR-006).
- We can demonstrate Auto Loader, Unity Catalog, Volumes, Delta Lake, and Workflows — every "killer" Databricks-idiomatic feature.

---

## ADR-002 — Google Gemini 2.5 Flash as the LLM

**Date:** Day 0
**Status:** Accepted

**Context.** We need an LLM for unstructured text extraction (vehicle data, competitors, objections, claim history) over ~153k messages, plus reasoning for the supervising agent. The LLM must be free-or-near-free, support structured output, handle PT-BR well, and have generous-enough rate limits to extract thousands of conversations per day.

**Decision.** Google Gemini 2.5 Flash via the `google-genai` SDK. Free-tier quotas (15 RPM, ~1500 RPD) are sufficient when paired with batched calls (~50 items per request) and persistent hash-based caching (ADR-006).

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

**Context.** Choice between LangGraph (standard agentic framework) and a custom Python control loop. We surveyed [`namastexlabs/agui-benchmark`](https://github.com/namastexlabs/agui-benchmark) which benchmarks 26 agent frameworks on the AG-UI protocol — a useful reference point given NMSTX's familiarity with the space.

**Decision.** Custom Python loop. The agent's flow — poll → diagnose → patch or escalate — is essentially linear with retries. A graph DSL adds abstraction without solving anything we don't already need to solve.

**Consequences.**
- Smaller dependency surface.
- Full control over retry, state, and idempotency semantics.
- We document the state machine explicitly in `agent/supervisor.py` instead of inferring it from a graph definition.
- The supervisor's control API (`pause`, `resume`, `inspect_failures`, `replay_run`) is structured to be wrappable as an MCP server later (see Roadmap in README).

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

## ADR-006 — Hash-based caching for LLM extraction

**Date:** Day 1
**Status:** Accepted

**Context.** Silver's LLM extraction (vehicle data, competitors, objection categories) is the most expensive step in the pipeline — both in Gemini free-tier quota burn and wall-clock time. Naïvely re-running extraction on every Silver refresh would either blow the 1,500 RPD quota inside an hour or force us to a paid tier. The pattern is well-established in NMSTX's own [`automagik-hive`](https://github.com/namastexlabs/automagik-hive) Smart CSV RAG (cited as ~450× faster reloads, 99% cost savings).

**Decision.** Persistent hash-keyed cache in `silver._extraction_cache` keyed by `(md5_of_concatenated_message_bodies, prompt_version)`. Every Silver run computes the hash per conversation; if the hash matches what's already cached, we skip the Gemini call entirely and read the cached extraction. New conversations and changed conversations get a fresh call.

**Consequences.**
- First full run touches all 15,000 conversations (~5 hours under free-tier limits with batching).
- Incremental runs only touch new/changed conversations — sub-minute typical refresh.
- `prompt_version` in the cache key means changing the extraction prompt triggers a full re-extract on next run (intentional).
- Cache table is part of the Silver schema so it lives in Unity Catalog, with all the lineage and access controls that come with it.
