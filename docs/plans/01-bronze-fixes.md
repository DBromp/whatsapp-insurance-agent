**Status:** Approved · **Owner:** TBD

---

# Day 1.5 — Bronze Fix-Up Plan

> **Note for VS Code Claude.** This is a focused cleanup pass on the Bronze layer before Day 2 starts. Day 1 shipped end-to-end and tests pass, but a code review surfaced four real bugs and three design gaps. This plan applies seven fixes across two files (plus auto-reconciled docs), in dependency order, with a verification step on Databricks. Apply these fixes straight — no pair-programming needed.

## Why this exists

The Bronze code review found that the current implementation does not actually behave the way ADR-005 promises. Specifically:

- The streaming write isn\'t awaited, so the post-write audit reads from a table that may still be empty.
- `cloudFiles.schemaEvolutionMode = "rescue"` captures drift silently, and `validate_schema_columns` is a tautology because it checks against the schema we declared ourselves. So "any drift fails the run" is currently false.
- `profile_dataframe` divides by zero on empty input and does three full scans per call.

Plus a few smaller doc/code mismatches. None of this blocks Day 2, but Day 2 will hammer `profile_dataframe` on every Silver and Gold run, and the agent on Day 4 reads `_pipeline_audit` for incident triage — so the data going into those tables needs to be correct.

This is a one-pass cleanup. Estimated effort: 60–90 minutes.

## Summary of changes

| # | Fix | File | Severity |
|---|---|---|---|
| 1 | Await the streaming query so the audit step reads the actual written rows | `notebooks/01_bronze_ingest.py` | Bug — wrong audit data |
| 2 | Switch to fail-fast drift detection so ADR-005\'s promise actually holds | `notebooks/01_bronze_ingest.py` | Bug — silent drift |
| 3 | Guard `profile_dataframe` against empty input (zero-row dataframes) | `src/profiling.py` | Bug — ZeroDivisionError |
| 4 | Compute `row_count` once and reuse; cap the dataframe scans | `src/profiling.py` | Performance |
| 5 | Replace hardcoded `anomalies=[]` with a clear TODO + comment | `notebooks/01_bronze_ingest.py` | Design clarity |
| 6 | Reconcile the notebook header table with the new drift behaviour | `notebooks/01_bronze_ingest.py` | Doc/code alignment |
| 7 | Differentiate interactive `RUN_ID`s so re-runs don\'t collide | `notebooks/01_bronze_ingest.py` | Audit hygiene |

Deferred to Day 2 or later (called out at the bottom): sys.path bootstrap robustness, channel filter, audit-table pre-creation.

## Sequence

Five steps in dependency order. Each is a stop-and-review point.

1. `src/profiling.py` — fixes #3 and #4 (no dependencies, smallest blast radius)
2. Optional new test: `tests/test_profiling.py` — pin the regression for #3 + smoke-test the function
3. `notebooks/01_bronze_ingest.py` — fixes #1, #2, #5, #6, #7 (the real ones)
4. `docs/decisions.md` — ADR-005 already says "fail-fast"; no change. ADR-006 cold-run estimate update (Day 2 plan Fix #12) can ride in this commit if VS Code Claude hasn\'t already done it.
5. Verify on Databricks (manual trigger, simulated drift, idempotency)

---

## Step 1 — `src/profiling.py` (Fixes #3 and #4)

**Goal:** `profile_dataframe` is safe on empty input and only counts the dataframe once.

**Current state:** lines 27 / 29 / 39 each trigger a full `.count()`. Line 27 divides by `df.count()` without zero-checking.

**Change spec:**

```python
def profile_dataframe(df: DataFrame, sample_n: int | None = None) -> dict[str, Any]:
    """Return a profile dict with shape, null rates, distinct counts, and basic stats.

    Args:
        df: Spark DataFrame to profile.
        sample_n: Optional row sample size for distinct-count computation.
                  Ignored if the DataFrame is empty.

    Returns:
        Dict with keys: row_count, columns, null_rates, distinct_counts.
        On an empty DataFrame, null_rates and distinct_counts are still returned
        but populated with zeros so the agent\'s downstream comparisons keep working.
    """
    row_count = df.count()  # 🔧 Fix #4 — count exactly once

    # 🔧 Fix #3 — short-circuit on empty
    if row_count == 0:
        return {
            "row_count": 0,
            "columns": df.columns,
            "null_rates": {c: 0.0 for c in df.columns},
            "distinct_counts": {c: 0 for c in df.columns},
        }

    # 🔧 Fix #4 — sample is now driven by the cached row_count, not a second .count() call
    if sample_n and sample_n < row_count:
        sample = df.sample(fraction=sample_n / row_count, seed=42)
        sample_count = sample_n  # approximate; sample() is probabilistic
    else:
        sample = df
        sample_count = row_count

    def _null_or_empty(col_name: str):
        cond = F.col(col_name).isNull()
        if isinstance(sample.schema[col_name].dataType, StringType):
            cond = cond | (F.col(col_name) == "")
        return cond

    null_rates = {
        c: sample.filter(_null_or_empty(c)).count() / max(sample_count, 1)
        for c in sample.columns
    }

    distinct_counts = {c: sample.select(c).distinct().count() for c in sample.columns}

    return {
        "row_count": row_count,
        "columns": sample.columns,
        "null_rates": null_rates,
        "distinct_counts": distinct_counts,
    }
```

**Why each change matters:**

- Caching `row_count` saves two full scans on cold caches. For a 153k-row Bronze that\'s pennies; for the agent on Day 4 polling every minute, it adds up fast.
- The empty-input guard returns a "shape-compatible" profile so `detect_anomalies` can still compare against a baseline without crashing. We could instead `raise`, but the agent will be calling this on possibly-empty per-batch slices, so silent-but-correct is better.
- `seed=42` on the sample makes profile output deterministic across runs given the same data — useful for the agent\'s anomaly detection (no false positives from sample-to-sample noise).

**Files touched:** `src/profiling.py` only.

---

## Step 2 (optional) — `tests/test_profiling.py`

We don\'t currently unit-test `profiling.py`. Adding three tests pins the regression for Fix #3 and gives the agent a leg to stand on:

1. `test_profile_empty_dataframe_does_not_divide_by_zero` — pass a 0-row Spark DF, assert no exception, assert returned dict has `row_count == 0`.
2. `test_profile_returns_expected_shape` — pass a tiny 5-row DF with known nulls, assert null_rates and distinct_counts are correct.
3. `test_detect_anomalies_flags_null_spike` — baseline with 0% nulls, current with 20% nulls in one column, assert a `null_rate_drift` anomaly is returned.

These tests need a `SparkSession` fixture — add `conftest.py` with a session-scoped fixture: `pytest.fixture(scope="session")` that builds `SparkSession.builder.appName("test").master("local[2]").getOrCreate()`.

**Cost:** ~50 lines + pyspark dep needs to be installable locally (it is — already in `requirements.txt`).
**Benefit:** Fix #3 has a permanent regression guard, and we have somewhere to add Silver/Gold profile tests later.

Optional. Skip if Daniel wants to keep moving.

---

## Step 3 — `notebooks/01_bronze_ingest.py` (Fixes #1, #2, #5, #6, #7)

**Goal:** the notebook actually does what ADR-005 promises — drift fails, audit runs after the write completes, each run is uniquely identifiable.

**Change 3a — header table (Fix #6, doc alignment).** Update the markdown header table to reflect the new drift behaviour:

```markdown
| Property | Value |
|---|---|
| Source | `/Volumes/nmstx_whatsapp_pipeline/bronze/raw_files/conversations/` |
| Sink | `nmstx_whatsapp_pipeline.bronze.messages` |
| Trigger | `availableNow=True` (batch-style for Workflows) |
| Drift policy | Fail-fast — any unexpected column raises before write (ADR-005) |
| Idempotent | Yes (Auto Loader checkpoint + Delta append) |
```

**Change 3b — RUN_ID (Fix #7).** When `jobRunId()` is undefined (interactive runs), suffix with a timestamp so re-runs are distinguishable in `_pipeline_audit`:

```python
from datetime import datetime, timezone

_job_run = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId()
if _job_run.isDefined():
    RUN_ID = str(_job_run.get())
else:
    RUN_ID = "local-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
```

**Change 3c — drift mode (Fix #2).** Switch Auto Loader from `rescue` to fail-fast. Two-line change:

```python
stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(BRONZE_SCHEMA)
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")  # 🔧 Fix #2
    .load(SOURCE_PATH)
)
```

With this, if a new parquet shows up with an extra column, Auto Loader raises before any write. The JobRun fails, `_pipeline_audit` doesn\'t get a success row, and the supervising agent on Day 4 sees a real failure to diagnose.

The existing `validate_schema_columns(...)` check below becomes redundant *for column presence* (Auto Loader handles that with the static schema), but it stays useful as a tripwire if anyone later loosens the schema mode. Keep it but add a one-line comment explaining its tripwire role.

**Change 3d — await termination (Fix #1).** Capture the streaming query and block on it before reading the table for the audit:

```python
query = (
    stream_df
    .withColumn("_ingest_date", F.current_date())
    .withColumn("_ingest_ts", F.current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "false")
    .partitionBy("_ingest_date")
    .trigger(availableNow=True)
    .toTable(TABLE)
)
query.awaitTermination()  # 🔧 Fix #1 — block until availableNow batch finishes
```

This is the most important change in the whole pass. Without it, the audit\'s `row_count` is wrong on every first run and on every run where Auto Loader picks up multiple files.

**Change 3e — anomalies TODO (Fix #5).** Replace the hardcoded empty list with an explicit TODO so it doesn\'t look intentional:

```python
# TODO(day-4 agent): wire detect_anomalies() against the previous successful
# run\'s profile (pulled from _pipeline_audit). Until then, audit rows show
# anomalies=[] for every run.
anomalies: list[dict] = []

write_audit_record(
    spark=spark,
    catalog=CATALOG,
    schema=BRONZE_SCHEMA_NAME,
    layer="bronze",
    job_run_id=RUN_ID,
    profile=profile,
    anomalies=anomalies,
    status="success",
)
```

(Also note: `job_run_id=str(RUN_ID)` becomes `job_run_id=RUN_ID` since we already string-typed it above. Tiny tidying.)

**Files touched:** `notebooks/01_bronze_ingest.py` only.

---

## Step 4 — `docs/decisions.md` reconciliation

ADR-005 already says: *"Bronze uses a static schema. Any drift fails the run and writes an audit record."* That matches the new fail-fast behaviour after Step 3 lands. **No edits needed.**

If VS Code Claude has not already updated ADR-006\'s cold-run estimate per the Day 2 review (Fix #12 there), this is a convenient commit to fold that one-line edit into: change "~5 hours for the cold first run" to "~25 min for the cold first run."

**Files touched:** `docs/decisions.md` (one-line edit only if not already done).

---

## Step 5 — Verify on Databricks

Push the changes, pull in Databricks Repo, manually trigger `bronze_ingest`. Five checks:

1. **Smoke test — happy path.** Run completes synchronously. `_pipeline_audit` has a new row with `status=success` and `profile_json` containing `row_count = 153228` (or current Bronze total, not zero or partial).
2. **Idempotency — re-run with no new files.** Trigger the job again. New `_pipeline_audit` row, but `row_count` matches step 1 (Auto Loader sees no new files, Delta sink no-ops, count is stable).
3. **Drift simulation — fail-fast.** Generate a small test parquet with a 15th column (`scripts/generate_test_parquet.py` plus one extra column). Upload to the Volume. Manually trigger the job. Expected: JobRun **fails** with `UnknownFieldException` or similar. No new row in `_pipeline_audit` with `status=success`. Delete the bad parquet, re-trigger, expect success.
4. **Audit content sanity.** `SELECT layer, status, run_id, created_at FROM bronze._pipeline_audit ORDER BY created_at DESC LIMIT 5` — confirms unique `run_id` per run (Fix #7) and the records make sense.
5. **Profiling sanity (no Databricks needed).** `pytest tests/` — all existing tests still pass. If we added Step 2\'s test file, the three new tests are green.

If all five pass, Bronze is genuinely production-shaped and Day 2 can start clean.

---

## Deferred — not in this pass

| Item | Why deferred |
|---|---|
| `sys.path` bootstrap robustness (lines 27–28) | Works today for `/Workspace/Repos/...` and `/Workspace/Users/...`. Fragile but not broken. Revisit if we ever hit it. |
| Defensive `channel == "whatsapp"` filter | Silver concern (Day 2). Belongs in `02_silver_transform.py`. |
| Pre-create `_pipeline_audit` with explicit DDL | Implicit creation works fine for v1. If we ever evolve the audit schema we\'ll need this; not yet. |

---

## Critical files & references

| File | Role |
|---|---|
| `notebooks/01_bronze_ingest.py` | Touched by 3a–3e |
| `src/profiling.py` | Touched by Step 1 |
| `tests/test_profiling.py` (new, optional) | Step 2 |
| `docs/decisions.md` | ADR-005 already aligns; optional ADR-006 estimate edit |
| `tests/test_schema.py` | Must continue to pass; no changes |
| Day 2 plan `DAY2_PLAN_REVIEWED.md` | These Bronze fixes are prerequisites for Day 2 Silver — Silver reads from `bronze.messages` and audits against `_pipeline_audit` |

## Worklog hand-off (for VS Code Claude)

After this lands, append to `docs/WORKLOG.md` (or wherever the dual-Claude coordination doc lives, if Daniel re-introduces it):

```
2026-05-28 @<author> — Bronze fix-up applied: streaming await, fail-fast drift,
profile zero-row guard + single-count optimization, TODO comment for anomalies,
RUN_ID disambiguation. ADR-005 promise now holds in code. tests green.
```

Tag the commit `v0.1.1-bronze-fixes` so Day 2 can branch off a known-good state.
