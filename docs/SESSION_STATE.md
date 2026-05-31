# Session state digest

> Living digest of where the project actually is right now, distinct from the static project context in `.claude/skills/whatsapp-insurance-pipeline/`. Read this on session start (Cowork or VS Code Claude) for "what\'s in flight, what\'s decided, what\'s blocked." Update at logical milestones — don\'t keep a running diary.

**Last updated:** end of Day 1 + Bronze experiment pass.

---

## Current build state

- **Bronze layer:** scaffolded, runs on Databricks Free Edition, ingests 153,228 rows + the 88-row drift batch. **NOT yet patched with the v2 fix-up plan** — `awaitTermination`, Option A drift detection, and the single-pass `profile_dataframe` rewrite are all still pending.
- **Silver layer:** not started. Patched plan exists at `docs/plans/02-day2-silver.md` (Approved, includes 12 inline review fixes, teach-mode stripped).
- **Gold + agent:** not started.
- **Test suite:** 14 passing (`test_schema.py` 11, `test_profiling.py` 3 pure-Python), 2 Spark-requiring tests skip gracefully without pyspark.
- **Smoke target:** `make smoke` runs `scripts/smoke_bronze.py` against a synthetic parquet — local end-to-end before Databricks push.
- **Repo:** `github.com/DBromp/whatsapp-insurance-agent`, pushed once successfully after `gh auth refresh -s workflow`. Branch is `master`. There are uncommitted changes locally (the tooling + plans commits I drafted earlier) — Daniel hasn\'t executed those git commands yet.
- **Databricks Free Edition:** catalog `nmstx_whatsapp_pipeline` exists with `bronze`, `silver`, `gold` schemas; Volume `bronze.raw_files` exists with the source parquet uploaded; secrets `nmstx-secrets/gemini-api-key` and `nmstx-secrets/pii-salt` exist and verified.

## Locked decisions (don\'t re-litigate)

- **Repo name:** `whatsapp-insurance-agent`
- **Platform:** Databricks Free Edition, plain PySpark (not DLT), Unity Catalog, Auto Loader, Workflows
- **LLM:** Gemini 2.5 Flash via `google-genai` SDK; batched calls; persistent cache (ADR-006)
- **Agent framework:** custom Python control loop, no LangGraph
- **Demo video:** yes, ~3 min on Day 4
- **Python target:** 3.12 (3.11 still supported)
- **PII masking:** stable token for names; CPF/CEP/phone/email/plate dimension-preserved
- **Drift detection (Option A from Bronze plan v2):** keep `cloudFiles.schemaEvolutionMode = "rescue"`, raise on non-null `_rescued_data` scoped to this run, audit-write-then-raise so the agent has diagnostic context

## Approved plans waiting to land (in priority order)

1. **`docs/plans/01-bronze-fixes.md`** (Approved v2) — 9 fixes including the critical `awaitTermination` + Option A drift detection. Empirically grounded — Experiment 2 confirmed Bug #1 produces lying audit data on every run. **Land this before Day 2 starts.**
2. **`docs/plans/02-day2-silver.md`** (Approved) — Silver implementation plan with 12 inline review fixes. Teach-mode stripped per Daniel\'s request. Reads `ops._pipeline_audit` as the watermark source (depends on Bronze Fix #8 landing first).

## Open findings not yet folded anywhere

These came out of the fresh-context sub-agent review and aren\'t fully addressed in current plans:

- **"Diff-and-patch" agent design is unrealistic.** `agent/supervisor.py` advertises `suggested_diff: str` against `notebook_source`. Reviewer\'s argument: Gemini will hallucinate diffs that won\'t apply; no test harness for patched notebooks before redeploy; 4-day timeline can\'t deliver. **Recommend descope to "classify + alert" (no auto-apply) before writing Day 4 code.** Update ADR-004 accordingly.
- **`body_hash` PII problem.** Pre-mask hash leaks PII; post-mask hash invalidates the entire cache on salt rotation. ADR-006 needs `pii_salt_version` in the cache key.
- **Three Gold tables are ML, not aggregations.** `gold.ghosting_predictors`, `gold.lead_intent_score`, `gold.conversation_quality` shouldn\'t be on the 15-min incremental refresh cadence. Suggested split: `gold.*` (aggregations) + `ml.*` (model outputs).
- **Silver→Gold objection refinement** (5-value Silver → 7-bucket Gold) needs the Silver cache blob to store richer message-level reasoning, not just the Literal label. Either fix or accept Day 3 re-extraction.
- **`incremental` mode partition equality is brittle at TZ boundaries.** Use `_silver_updated_at` watermark instead of partition equality.

## Working conventions Daniel has set

- **Explicit approval required before file changes** — pushed back twice when I executed without asking. Default to plan-then-confirm.
- **No teach-mode / pair-programming** — wants direct execution. Plans should not include "Concepts taught" sections or alternating Claude/Daniel function tables.
- **Plans live in `docs/plans/`** with status header (`Draft | Approved | In Progress | Done`) and `NN-short-name.md` filename pattern.
- **Conventional commits with scope tags:** `feat(cowork): ...`, `fix(vscode): ...`, etc.
- **Run on Databricks before patching** — Daniel\'s strong instinct, validated by Experiment 2 (refuted a reviewer assumption).
- **Use fresh-context sub-agent reviews** — established as standing practice for the next layer. Done once for Bronze (found 7 critical + 7 plan-level issues). Do again before Silver locks.

## Tools and skill files Claude should re-read at session start

1. `.claude/skills/whatsapp-insurance-pipeline/SKILL.md` and references (static project context)
2. `docs/plans/00-project-overview.md` (4-day plan)
3. `docs/decisions.md` (ADRs)
4. **This file** (`docs/SESSION_STATE.md`) (current flux state)
5. The "Status" header of each `docs/plans/NN-*.md` to see what\'s in flight

## What\'s next (immediate)

1. Push the uncommitted tooling + plans changes (three commits drafted, see the chat history Daniel will lose at session end — or just `git status` and decide commit groupings on the fly)
2. Apply Bronze fix-up plan v2 (`docs/plans/01-bronze-fixes.md`) — VS Code Claude can pick this up
3. Run Experiments 3 + 4 (optional confirmation) once fix v2 is deployed
4. Start Day 2 Silver from a known-good Bronze
