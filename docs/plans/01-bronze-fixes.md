**Status:** Approved (revised post-Databricks experiments + post-VS-Code-Claude review) · **Owner:** TBD

---

# Bronze Fix-Up Plan (v2.1 — empirically grounded + review-corrected)

## Revisions

- **v2.1 (this rev):** Fix #9 (`validate_enum_values`) rewritten — original `limit()` + set-difference could return false negatives because Spark's arbitrary `limit` may pick only allowed values. Corrected version filters Spark-side first, then bounds the collect. Fix #4 test gets a `pytest.approx` tweak so HLL low-cardinality edge cases don't flake. Fix #8 ordering made explicit.
- **v2:** Replaced Fix #2 with Option A (keep `rescue` + raise on non-null `_rescued_data`) after Databricks Experiment 2 confirmed rescue mode works correctly and Bug #1 is more consequential than originally thought.
- **v1 (superseded):** Recommended switching to `failOnNewColumns`, which the empirical run showed wasn't needed.

> **Note for VS Code Claude.** This is a revision of the original Bronze fix-up plan, updated after two Databricks experiments confirmed one critical bug, refuted one assumption, and a fresh-context code review surfaced additional issues. Apply these fixes straight — no pair-programming needed.

## Why this version supersedes v1

The original plan recommended switching `cloudFiles.schemaEvolutionMode` from `"rescue"` to `"failOnNewColumns"` based on a code review claim that rescue mode silently dropped drift under static `.schema()`. We ran the experiment instead of guessing — **rescue mode actually works correctly** under static schema, and the audit row from the experiment also revealed that **the streaming-write race condition (Fix #1) is even more consequential than originally thought** because it produces internally inconsistent audit data (row_count=0 alongside real distinct_counts on every successful run).

So Fix #2 is rewritten as **Option A**: keep `rescue` mode (it gives us forensic visibility via `_rescued_data`), add an explicit post-write raise that fires when any rescued rows landed in *this run\'s* batch. Best of both worlds — fail-fast for the agent, full drift context preserved for diagnosis.

## Empirical findings from the Databricks experiments

**Experiment 1 (baseline + drift parquet, rescue mode, current code):**

| Observation | Implication |
|---|---|
| Drift batch (88 rows, 15 cols) ran to **SUCCESS** | ADR-005\'s "any drift fails the run" is currently false |
| `_rescued_data` column populated with the drift content, including `_file_path` for forensics | Rescue mode works as documented under static schema — reviewer\'s suspicion was wrong |
| Audit row shows `row_count: 0` but `distinct_counts: { message_id: 88, conversation_id: 20, ... }` | **Bug #1 confirmed and consequential** — race between stream commit and audit reads produces lying audit data on every run |
| `null_rates` all 0.0 (every column) | Confirms the race: when `df.count()` returns 0, the `max(sample_count, 1)` denominator forces every rate to 0/1 = 0.0 |
| `validate_schema_columns` did not raise (run status = success) | Confirmed tautology — the check is a no-op because Auto Loader enforces the static schema we declared |

Experiment 3 (failOnNewColumns) and Experiment 4 (empty file) deferred — we have enough data to design fixes without them. Optionally rerun once the patched code is deployed to confirm the new behavior.

## Summary of changes (v2)

| # | Fix | File | Severity | Status |
|---|---|---|---|---|
| 1 | Await the streaming query so the audit reads post-commit data | `notebooks/01_bronze_ingest.py` | **Critical** (empirically confirmed) | Required |
| 2 | **Option A** — keep `rescue` mode, raise on non-null `_rescued_data` in this run\'s batch | `notebooks/01_bronze_ingest.py` | **Critical** (ADR-005 promise) | Required |
| 3 | Empty-DF guard in `profile_dataframe` | `src/profiling.py` | Bug | **Done** (already landed via lazy-import refactor) |
| 4 | Single-pass `agg` rewrite of `profile_dataframe` + `approx_count_distinct` | `src/profiling.py` | Performance — bites at Silver/Gold scale | Required |
| 5 | Wire actual `anomalies` (drift signal from Fix #2) into the audit instead of `anomalies=[]` | `notebooks/01_bronze_ingest.py` | Design clarity | Required |
| 6 | Reconcile notebook header + ADR-005 with new drift behaviour | `notebooks/01_bronze_ingest.py`, `docs/decisions.md` | Doc/code alignment | Required |
| 7 | Disambiguate interactive `RUN_ID`s with timestamp suffix | `notebooks/01_bronze_ingest.py` | Audit hygiene | Required |
| 8 | Move `_pipeline_audit` from `bronze` schema to a shared `ops` schema | `notebooks/01_bronze_ingest.py`, `src/profiling.py` | Day 2/3 prerequisite | Required |
| 9 | Bound `validate_enum_values` collect (Spark-side filter, not limit-then-diff) | `src/schema.py` | Future Day 2/3 footgun | Recommended |

Deferred (Day 2/4 concerns): defensive `channel == "whatsapp"` filter, pre-create audit DDL, `sys.path` bootstrap robustness, audit-table Z-ORDER/clustering, TZ on `timestamp` column.

## Sequence

Apply in order. Steps 1–7 are one logical commit per file; Step 8 is a small schema change that needs the catalog/schema created first; Step 9 is independent.

1. `src/profiling.py` — Fixes #4 (single-pass agg rewrite). Fix #3 is already done.
2. `notebooks/01_bronze_ingest.py` — Fixes #1, #2, #5, #6, #7 in one pass.
3. `docs/decisions.md` — ADR-005 update to reflect Option A behaviour. ADR-006 cold-run estimate update if not already done (Day 2 plan Fix #12).
4. New `ops` schema — Fix #8.
5. `src/schema.py` — Fix #9 (small).
6. Verify on Databricks — re-run drift parquet, confirm raise + audit + new schema.

---

## Step 1 — `src/profiling.py` Fix #4 (single-pass `profile_dataframe`)

**Goal:** one full table scan per profile call instead of ~32. Use `agg` with per-column expressions and `approx_count_distinct` (HyperLogLog, single pass).

**Change spec:**

```python
def profile_dataframe(df: "DataFrame", sample_n: int | None = None) -> dict[str, Any]:
    """One-pass profile of a Spark DataFrame.

    Returns shape + null rates + (approximate) distinct counts in a single agg over
    the input. Trades exact distinct counts for HyperLogLog approximation (default
    rsd=0.05) so this stays cheap on multi-million-row Silver/Gold tables.

    Empty-DataFrame returns a zero-shape dict so the agent\'s downstream comparisons
    keep working without special-casing.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    columns = df.columns

    # Build per-column agg expressions in one pass.
    agg_exprs = [F.count(F.lit(1)).alias("__row_count__")]
    for c in columns:
        is_string = isinstance(df.schema[c].dataType, StringType)
        null_cond = F.col(c).isNull() | (F.col(c) == "") if is_string else F.col(c).isNull()
        agg_exprs.append(F.sum(F.when(null_cond, 1).otherwise(0)).alias(f"__null_{c}__"))
        agg_exprs.append(F.approx_count_distinct(F.col(c), rsd=0.05).alias(f"__dc_{c}__"))

    row = df.agg(*agg_exprs).collect()[0]
    row_count = row["__row_count__"]

    if row_count == 0:
        return {
            "row_count": 0,
            "columns": columns,
            "null_rates": {c: 0.0 for c in columns},
            "distinct_counts": {c: 0 for c in columns},
        }

    null_rates = {c: row[f"__null_{c}__"] / row_count for c in columns}
    distinct_counts = {c: row[f"__dc_{c}__"] for c in columns}

    return {
        "row_count": row_count,
        "columns": columns,
        "null_rates": null_rates,
        "distinct_counts": distinct_counts,
    }
```

**Notes:**

- Drops the `sample_n` sampling branch entirely — `approx_count_distinct` is fast enough on full tables that sampling adds complexity without proportional benefit. If we later need it, add it back as a deterministic Bernoulli sample with `.cache()`.
- `rsd=0.05` means distinct counts are accurate to ±5%, which is good enough for anomaly detection. Tighter (0.01) costs more memory.
- Tests in `tests/test_profiling.py` should still pass — the empty-DF test is unchanged. **Defensive tweak:** also update the 5-row distinct-count assertion from `== 5` to `== pytest.approx(5, abs=1)`. HyperLogLog is exact for tiny cardinalities in current Spark versions, but the `pytest.approx(5, abs=1)` tolerance avoids future flakes if that behaviour changes.

---

## Step 2 — `notebooks/01_bronze_ingest.py` Fixes #1, #2, #5, #6, #7

**Change 2a — header table (Fix #6).** Update the markdown header to reflect the new drift behaviour:

```markdown
| Property | Value |
|---|---|
| Source | `/Volumes/nmstx_whatsapp_pipeline/bronze/raw_files/conversations/` |
| Sink | `nmstx_whatsapp_pipeline.bronze.messages` |
| Trigger | `availableNow=True` (batch-style for Workflows) |
| Drift policy | Captured to `_rescued_data` for forensics, then raised so the agent sees a real failure (ADR-005, Option A) |
| Idempotent | Yes (Auto Loader checkpoint + Delta append; awaitTermination ensures the audit reads committed data) |
```

**Change 2b — RUN_ID disambig (Fix #7).** When `jobRunId()` is undefined, suffix with a UTC timestamp:

```python
from datetime import datetime, timezone

_job_run = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId()
if _job_run.isDefined():
    RUN_ID = str(_job_run.get())
else:
    RUN_ID = "local-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

RUN_START = datetime.now(timezone.utc)  # used by Fix #2 to scope drift detection to this run
```

**Change 2c — keep rescue mode (Fix #2 setup).** Leave the Auto Loader reader as-is — `cloudFiles.schemaEvolutionMode = "rescue"`. The experiment confirmed this works.

The existing `validate_schema_columns` check below stays in place as a tripwire (it can\'t fire today, but if someone later removes the static schema and Auto Loader starts inferring, this would catch the wrong columns). Add a one-line comment explaining its tripwire-only role.

**Change 2d — await termination + drift detection + audit (Fixes #1, #2, #5).** This is the main change. Replace the existing write block + audit block with:

```python
# --- Write Bronze (Fix #1 — block on streaming completion) ---
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
query.awaitTermination()

# --- This-run batch (used by both audit and drift detection) ---
this_run_batch = spark.table(TABLE).filter(F.col("_ingest_ts") >= F.lit(RUN_START))

# --- Fix #2 (Option A) — drift detection on this run only ---
rescued = this_run_batch.filter(F.col("_rescued_data").isNotNull())
rescued_count = rescued.count()
anomalies: list[dict] = []
if rescued_count > 0:
    samples = [r["_rescued_data"] for r in rescued.select("_rescued_data").limit(5).collect()]
    anomalies.append({
        "type": "schema_drift",
        "rescued_row_count": rescued_count,
        "examples": samples,
    })

# --- Audit (Fix #5 — wire real anomalies through) ---
profile = profile_dataframe(this_run_batch)
status = "drift_detected" if rescued_count > 0 else "success"

write_audit_record(
    spark=spark,
    catalog=CATALOG,
    schema="ops",  # Fix #8 — moved to shared ops schema
    layer="bronze",
    job_run_id=RUN_ID,
    profile=profile,
    anomalies=anomalies,
    status=status,
)

print(f"Bronze ingest complete: {profile['row_count']} rows. status={status}")

# --- Now raise so the JobRun fails and the agent picks up via the audit row ---
if rescued_count > 0:
    raise ValueError(
        f"Schema drift detected: {rescued_count} rows in this run\'s batch landed in _rescued_data. "
        f"See nmstx_whatsapp_pipeline.ops._pipeline_audit (run_id={RUN_ID}) for details. "
        f"Examples: {samples}"
    )
```

**Why the order is audit-first-then-raise:** the agent reads `_pipeline_audit` to diagnose failures. If we raise before writing the audit, the agent has no diagnostic context. Writing first means the audit captures the drift evidence, *then* the run fails — the agent has everything it needs.

---

## Step 3 — `docs/decisions.md` updates

**ADR-005 — replace the "Decision" + "Consequences" sections with:**

> **Decision.** Bronze uses a static schema with `cloudFiles.schemaEvolutionMode = "rescue"`. Drifted columns land in `_rescued_data` for forensic visibility. A post-write check raises if any rows in the current run\'s batch have non-null `_rescued_data`, causing the JobRun to fail so the supervising agent has a real failure to diagnose. The audit row is written *before* the raise so the agent has full drift context (rescued row count + sample rescued values) when it polls for failures.
>
> **Consequences.**
> - Bronze is brittle by design. That brittleness is what makes the agent\'s value visible.
> - Drift data is preserved (in `_rescued_data`) rather than lost, so the agent can propose informed schema patches.
> - The drift detection is scoped to `_ingest_ts >= RUN_START` so a single bad batch doesn\'t cause every future run to fail forever — once the schema is patched, subsequent runs are clean.
> - The earlier `validate_schema_columns` check stays as a tripwire for the case where someone later removes the static `.schema()` declaration. Under the current code it can\'t fire.

**ADR-006 — cold-run estimate update** (Day 2 plan Fix #12, if not already done): change "~5 hours for the cold first run" to "~25 min" since the batching design lands it under free-tier limits comfortably.

---

## Step 4 — New `ops` schema (Fix #8)

> **⚠️ Ordering requirement.** This step MUST happen before the Step 2 code change lands in Databricks. The patched notebook writes to `ops._pipeline_audit`; if that schema/table doesn't exist when the run fires, the audit-write fails and Bronze breaks. Three-step ordering:
>
> 1. Run the `CREATE SCHEMA` + `DEEP CLONE` SQL below in Databricks first
> 2. Verify `ops._pipeline_audit` exists and has the historical rows
> 3. Then push the code change so the next Bronze run lands its audit in the new location
>
> Only after confirming new rows are landing in `ops._pipeline_audit` should the old `bronze._pipeline_audit` be dropped.

Rationale: the audit table currently lives in `bronze._pipeline_audit` regardless of which layer writes to it. When Silver and Gold start writing audits, they'd either fragment (one audit table per layer) or all write to `bronze._pipeline_audit` (semantically wrong — a Silver run's audit doesn't belong to the Bronze schema). Move to a shared `ops` schema instead.

SQL to run in Databricks before the code change:

```sql
CREATE SCHEMA IF NOT EXISTS nmstx_whatsapp_pipeline.ops;

-- Migrate existing audit rows
CREATE OR REPLACE TABLE nmstx_whatsapp_pipeline.ops._pipeline_audit
DEEP CLONE nmstx_whatsapp_pipeline.bronze._pipeline_audit;

-- Verify, then drop the old one
DROP TABLE IF EXISTS nmstx_whatsapp_pipeline.bronze._pipeline_audit;
```

Then update `write_audit_record` callers to pass `schema="ops"` (already shown in Step 2 above).

---

## Step 5 — `src/schema.py` Fix #9 (bounded `validate_enum_values`)

Replace the unbounded collect. **Important:** filter the allowed values on the Spark side *before* the `limit`, otherwise the `limit` could arbitrarily pick only allowed values and return a false-negative empty result.

```python
def validate_enum_values(df, column, allowed, max_violations=20):
    """Return up to `max_violations` unexpected values in `column`. Empty list = valid.

    Filters out allowed values on the Spark side so the bounded collect cannot
    hide violations behind a sample of allowed values.
    """
    from pyspark.sql import functions as F

    unexpected_df = (
        df.select(column)
        .distinct()
        .filter(F.col(column).isNotNull())
        .filter(~F.col(column).isin(list(allowed)))
        .limit(max_violations)
    )
    return sorted(r[column] for r in unexpected_df.collect())
```

Now safe to call on `metadata` or any other high-cardinality column without OOM risk **and** correct under all data distributions.

> **Why the earlier draft was wrong:** an earlier version did `df.select(column).distinct().limit(N).collect()` then set-difference against `allowed`. Spark's `limit` is unordered, so if a column had thousands of distinct values where many were unexpected, the `limit` could happen to pick only allowed values — the set-difference would then return empty, falsely claiming "no violations" on a column that had hundreds of drift values. Pushing the filter to the Spark side fixes this because the collect can only contain unexpected rows.

---

## Step 6 — Verify on Databricks

After pushing the changes and pulling in Databricks Repo, manually trigger `bronze_ingest`. Five checks:

1. **Happy path re-run.** Trigger the job on a Volume that contains only valid 14-column files. Confirm: JobRun = success, `_pipeline_audit` (now in `ops`) shows the right `row_count` (no longer 0), `null_rates` are real (not all 0.0), `anomalies=[]`, `status=success`.
2. **Idempotency.** Re-run with no new files. Confirm new audit row, `row_count = 0` (legitimately — no new data this run), `status=success`.
3. **Drift re-run.** Upload `test_drift.parquet` again, trigger. Confirm: JobRun **fails** with the drift ValueError, `_pipeline_audit` has a new row with `status=drift_detected` and `anomalies_json` containing the rescued samples.
4. **Recovery after schema patch.** Add `reply_to_id` to `BRONZE_COLUMNS` in `src/schema.py`, push, pull, re-trigger. Auto Loader now expects the column → no rescue → `status=success`, drift batch is consumed cleanly.
5. **Unit tests still green.** `make test` runs all 14 schema + anomaly tests plus the Spark-requiring profiling tests if pyspark is available locally.

If all five pass, ADR-005 finally holds in code, the audit row is trustworthy on every run, and Day 2 Silver can start clean from `ops._pipeline_audit` as the canonical run-state source.

---

## Worklog hand-off

After this lands, append to the commit message:

```
fix(bronze): empirically-grounded fix-up pass (v2)

- Add awaitTermination so audit reads post-commit data (Fix #1 — was producing
  row_count=0 alongside real distinct_counts on every successful run)
- Implement Option A drift detection: keep rescue mode, raise on non-null
  _rescued_data in this run\'s batch (Fix #2 — ADR-005 promise now holds)
- Single-pass agg rewrite of profile_dataframe with approx_count_distinct
  (Fix #4 — was ~32 full scans per call)
- Move _pipeline_audit to shared ops schema (Fix #8 — Silver/Gold prerequisite)
- Wire real drift anomalies into audit instead of anomalies=[] (Fix #5)
- Disambiguate interactive RUN_IDs with UTC timestamp (Fix #7)
- Bound validate_enum_values collect (Fix #9)
- Reconcile notebook header + ADR-005 with new drift behaviour (Fix #6)
```

Tag the commit `v0.1.2-bronze-fixes-v2` so Day 2 can branch off a known-good state.

## Critical files & references

| File | Role |
|---|---|
| `notebooks/01_bronze_ingest.py` | Touched by 2a–2d |
| `src/profiling.py` | Touched by Step 1 |
| `src/schema.py` | Touched by Step 5 |
| `docs/decisions.md` | ADR-005 rewrite, ADR-006 estimate fix |
| Databricks UI | New `ops` schema, Volume re-trigger to verify |
| `tests/test_profiling.py` | Should still pass after the agg rewrite |
| `docs/plans/02-day2-silver.md` | This is prerequisite — Silver reads `ops._pipeline_audit` as the watermark source |
