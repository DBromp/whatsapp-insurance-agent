# NMSTX Technical Test — 4-Day Delivery Plan

**Owner:** Daniel Brom
**Stack:** Databricks Free Edition · Google Gemini 2.5 Flash · PySpark · Pure Python · GitHub
**Deliverable:** Public GitHub repo with a live, self-healing medallion pipeline + supervising AI agent
**Timeline:** 4 days, ~8h/day target

---

## Day 0 — Pre-flight (today)

Already done:
- Read brief + data dictionary, profiled the data (153,228 messages, 15,000 conversations)
- Locked tech stack, catalog naming (`nmstx_whatsapp_pipeline`), and 8 Gold-layer insights
- Built and installed the `whatsapp-insurance-pipeline` project-context skill

Still to do before Day 1 starts:
- Install the skill (one click on the card above)
- Pick the GitHub repo name (suggested: `nmstx-whatsapp-pipeline`)
- Verify Gemini API key works (one curl from terminal)
- Verify Databricks Free Edition workspace is reachable
- Sign off on this plan

---

## Day 1 — Foundation (Bronze + repo + infra)

**Goal:** Bronze layer is live in Databricks, incrementally ingesting from a Volume; repo is scaffolded and pushed; secrets and catalog wired up.

| # | Task | Est. | Output |
|---|---|---|---|
| 1.1 | Create Unity Catalog `nmstx_whatsapp_pipeline` + schemas `bronze`, `silver`, `gold` | 20m | Catalog visible in workspace |
| 1.2 | Create Volume `bronze.raw_files` and upload `conversations_bronze.parquet` into `/conversations/` | 20m | File accessible via `dbfs:/Volumes/...` |
| 1.3 | Add secrets: `gemini-api-key`, `pii-salt` (random 32-byte string) | 15m | Secrets scope `nmstx-secrets` populated |
| 1.4 | Scaffold local repo: `src/`, `notebooks/`, `tests/`, `agent/`, `pyproject.toml`, `requirements.txt`, `.gitignore`, `README.md` skeleton, `LICENSE` | 45m | Repo initialized locally |
| 1.5 | Build `01_bronze_ingest.py` notebook — Auto Loader from Volume → `bronze.messages` Delta table, append-only, partitioned by `_ingest_date` | 90m | Bronze table populated |
| 1.6 | Schema validation: enforce expected 14 columns, fail loudly on drift, log to `_pipeline_audit` table | 45m | Schema-safe ingestion |
| 1.7 | Build reusable `bronze_profile.py` module — null rates, distributions, anomaly detection (reused later by the agent) | 45m | Profiling utilities |
| 1.8 | Push to GitHub, run first Workflow trigger to verify ingestion end-to-end | 30m | First green run on GitHub + Databricks |
| 1.9 | Smoke test: drop a 100-row synthetic parquet into the Volume, confirm it appears in `bronze.messages` without manual intervention | 30m | Incremental Bronze confirmed |

**Day 1 Definition of Done:** `bronze.messages` contains 153,228 rows from the initial parquet, plus the synthetic test rows. Schema is enforced. Repo is on GitHub. Auto Loader is wired.

---

## Day 2 — Silver (cleaning, PII, extraction)

**Goal:** Silver layer is live with cleaned, masked, enriched data. PII masking is deterministic and dimension-preserving. LLM extraction works in batches under Gemini free-tier limits.

| # | Task | Est. | Output |
|---|---|---|---|
| 2.1 | PII masking module `src/pii.py` — Brazilian patterns (CPF/CEP/phone/email/plate), deterministic, dimension-preserving, salt-driven | 90m | `pii.mask_text(s)` + `pii.mask_phone(s)` |
| 2.2 | PII unit tests — 30 cases incl. malformed/edge inputs, idempotency, dimension preservation | 45m | `tests/test_pii.py` passing |
| 2.3 | Build `02_silver_messages.py` notebook — parse timestamp, explode metadata JSON, mask PII, dedupe by message_id, derived columns | 90m | `silver.messages` populated |
| 2.4 | Gemini extraction client `src/gemini.py` — batched, structured output via Pydantic, retry with backoff, rate-limit aware | 60m | `gemini.extract_batch(items, schema)` |
| 2.5 | Conversation-level LLM extraction — vehicle (brand/model/year), competitors, sinistro flag, objection category. Batched ~50 conversations/call | 90m | Extraction cache in `silver._extraction_cache` |
| 2.6 | Build `silver.conversations` rollup — aggregate messages → conversation-level facts + LLM-extracted fields | 60m | `silver.conversations` populated |
| 2.7 | Idempotency tests — re-run Silver job, confirm zero new rows written (MERGE invariant) | 30m | Idempotency confirmed |
| 2.8 | Wire `silver_transform` Databricks Job, schedule every 15 min | 15m | Scheduled silver job |

**Day 2 Definition of Done:** `silver.messages` and `silver.conversations` populated. Zero PII visible in either table. LLM extraction cached so reruns are cheap. Silver job runs on schedule.

---

## Day 3 — Gold layer + incremental refresh

**Goal:** All 8 Gold tables built and refreshing incrementally when Silver advances its watermark.

| # | Task | Est. | Output |
|---|---|---|---|
| 3.1 | `gold.agent_scorecard` — pure aggregation, `low_sample` flag, exclude `em_negociacao` from rates | 45m | Table populated |
| 3.2 | `gold.competitor_matrix` — competitor mention × outcome cross-tab, win rate | 45m | Table populated |
| 3.3 | `gold.vehicle_cohorts` — brand+segment+year_band grouping, close rate per cohort | 45m | Table populated |
| 3.4 | `gold.ghosting_predictors` — logistic correlations or simple sklearn LogReg over Silver features | 60m | Predictors table |
| 3.5 | `gold.funnel_stages` — Gemini stage classifier per message, aggregate to conversation, dropoff stage | 90m | Funnel table |
| 3.6 | `gold.objection_taxonomy` — LLM-driven taxonomy assignment + resolution flag | 60m | Taxonomy table |
| 3.7 | `gold.lead_intent_score` — composite weighted score, calibrated against outcome (hot ≥3× cold close rate) | 60m | Score table |
| 3.8 | `gold.conversation_quality` — Gemini rubric scoring per conversation, dimension breakdown | 60m | Quality table |
| 3.9 | Incremental refresh wiring — watermark column on Silver, Gold jobs read only new since last watermark | 45m | Incremental wiring complete |
| 3.10 | End-to-end test — drop new parquet into Volume, watch Bronze→Silver→Gold cascade automatically | 30m | Live propagation confirmed |

**Day 3 Definition of Done:** Every Gold table populated. Adding a new parquet to the Volume triggers the full pipeline through to Gold within one refresh cycle.

---

## Day 4 — Agent + polish + ship

**Goal:** Self-healing supervising agent is running. Documentation is polished. Repo is shipped.

| # | Task | Est. | Output |
|---|---|---|---|
| 4.1 | Agent architecture `agent/supervisor.py` — control loop, state machine, polls Databricks SDK for JobRuns | 90m | Agent skeleton |
| 4.2 | Failure-capture: when a JobRun fails, pull stderr + last 200 log lines + notebook source | 30m | `agent.capture_failure(run_id)` |
| 4.3 | Diagnosis via Gemini — prompt: traceback + notebook source → diagnosis + patch proposal as structured output | 90m | `agent.diagnose(failure)` |
| 4.4 | Safe-patch classifier — categorize patches as auto-applicable (schema additions, null-handling, retry config) vs. human-required (logic changes) | 45m | Patch-safety logic |
| 4.5 | Auto-apply path — write patched notebook to a branch, run, verify, merge if green; else rollback | 60m | Auto-apply flow |
| 4.6 | Escalation path — structured alert (markdown summary + suggested patch) emitted to a `_pipeline_alerts` Delta table; future hook for Slack/email | 30m | Alert table |
| 4.7 | Failure-injection tests — corrupt parquet, schema drift, LLM timeout, Spark OOM. Confirm agent diagnoses each | 60m | Test results doc |
| 4.8 | README — quickstart, architecture diagram (mermaid), data model, agent behavior, demo screenshots | 75m | Polished README |
| 4.9 | Decision log `docs/decisions.md` — every design choice with rationale (signals seniority to the assessor) | 30m | Decision log |
| 4.10 | Tag v1.0, final commit, public repo URL ready to share | 30m | Repo shipped |

**Day 4 Definition of Done:** Public GitHub repo with a working pipeline, a supervising agent that has demonstrably caught and either fixed or escalated at least one injected failure, polished documentation, and a v1.0 tag.

---

## Cross-cutting concerns (runs across all 4 days)

**Testing strategy:** PII masking gets unit tests on Day 2. Pipeline transforms get integration tests that run on a 100-row sample. End-to-end test runs the full pipeline on a 1,000-row slice. The agent gets failure-injection tests on Day 4.

**Documentation cadence:** Every notebook starts with a docstring header (purpose, inputs, outputs, idempotency). Every Python module has docstrings on public functions. README updated at end of each day. Decision log appended whenever a non-obvious tradeoff is made.

**Commit discipline:** Commit at end of each Pomodoro-sized task (every 45–90 min). Conventional commit messages (`feat:`, `fix:`, `docs:`). Tag `v0.1` after Day 1, `v0.2` after Day 2, etc.

**Compute budget:** Databricks Free Edition has a monthly serverless compute cap. LLM extraction results are persistently cached in `silver._extraction_cache` keyed by `(text_hash, prompt_version)` so reruns don't reburn compute.

---

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Gemini rate limits stall extraction | Medium | High | Batch ~50 items per call; persistent cache; Groq Llama-3.3-70B free-tier fallback |
| Databricks compute cap exceeded mid-build | Low | High | Cache aggressively; develop on 1k-row subset, full run only at end of each day |
| PII masking breaks downstream regex extraction | Medium | Medium | Extract entities first, then mask — order matters |
| Agent self-healing infinite-loops on a bad patch | Medium | High | Max 3 patch attempts per JobRun, then mandatory escalation |
| LLM stage/objection classification is noisy | High | Medium | Few-shot prompt with 20 examples per class; manual eval on 50 conversations before scaling |
| Audio transcripts contain ASR errors that confuse extraction | Medium | Low | Filter `message_type='audio'` from LLM extraction OR add a low-confidence flag |
| Time overrun on Day 4 polish | High | Medium | Ship MVP of each Gold table first; iterate only if Day 4 has slack |

---

## Locked decisions

1. **GitHub repo name:** `whatsapp-insurance-agent`
2. **Pipeline style:** Plain PySpark for both Silver and Gold (Workflows + Auto Loader + MERGE patterns). DLT considered and deferred — its abstracted failure model would make self-healing harder, and we already get strong Databricks bonus credit from Unity Catalog + Auto Loader + Delta + Workflows.
3. **Agent framework:** Custom Python control loop. No LangGraph — our flow (monitor → diagnose → patch or escalate) doesn't need a graph DSL.
4. **Demo video:** Yes — ~3 min Loom walkthrough at the end of Day 4, showing live pipeline + agent self-healing.

---

## Anchor metrics for "done well"

By the end of Day 4, we should be able to point at:

- 153k+ rows flowing Bronze → Silver → Gold with zero manual intervention
- Adding a new parquet to the Volume propagates to all 8 Gold tables within 30 min
- At least one injected failure caught by the agent and resolved automatically
- At least one injected failure caught by the agent and escalated correctly
- README + architecture diagram + decision log present and clear
- Public repo URL ready to send to the assessor
- Total monthly Databricks compute usage well under the Free Edition cap
