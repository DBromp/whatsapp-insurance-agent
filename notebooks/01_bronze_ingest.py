# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC Streams WhatsApp message parquet files from the Unity Catalog Volume into
# MAGIC `bronze.messages`. Schema is enforced — any drift fails the run and emits an
# MAGIC audit record so the supervising agent can act on it.
# MAGIC
# MAGIC | Property | Value |
# MAGIC |---|---|
# MAGIC | Source | `/Volumes/nmstx_whatsapp_pipeline/bronze/raw_files/conversations/` |
# MAGIC | Sink | `nmstx_whatsapp_pipeline.bronze.messages` |
# MAGIC | Trigger | `availableNow=True` (batch-style for Workflows) |
# MAGIC | Drift policy | Captured to `_rescued_data` for forensics, then raised so the agent sees a real failure (ADR-005, Option A) |
# MAGIC | Idempotent | Yes (Auto Loader checkpoint + Delta append; `awaitTermination` ensures the audit reads committed data) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
import sys

# Make `src/` importable when this runs as a Databricks notebook.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.insert(0, "/Workspace" + "/".join(notebook_path.split("/")[:-2]))

from src.schema import BRONZE_COLUMNS, build_bronze_schema, validate_schema_columns
from src.profiling import profile_dataframe, write_audit_record

BRONZE_SCHEMA = build_bronze_schema()

CATALOG = "nmstx_whatsapp_pipeline"
BRONZE_SCHEMA_NAME = "bronze"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA_NAME}.messages"

SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA_NAME}/raw_files/conversations/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA_NAME}/_checkpoints/messages/"

# 🔧 Fix #7 — interactive runs get a timestamped suffix so they don't collide in _pipeline_audit.
_job_run = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId()
if _job_run.isDefined():
    RUN_ID = str(_job_run.get())
else:
    RUN_ID = "local-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Capture run-start so drift detection (Fix #2 Option A) only inspects rows
# written by *this* run, not historical drift left over from earlier batches.
RUN_START = datetime.now(timezone.utc)

# Audit table now lives in a shared `ops` schema (Fix #8) so Silver and Gold
# can write the same shape without polluting the `bronze` namespace.
AUDIT_SCHEMA = "ops"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read with Auto Loader

# COMMAND ----------

stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(BRONZE_SCHEMA)
    # 🔧 Fix #2 (Option A) — keep `rescue` mode so drifted columns land in
    # `_rescued_data` for forensic visibility. A post-write check below raises
    # if any rows in *this* run's batch have non-null `_rescued_data`, so
    # ADR-005's fail-fast promise still holds — just with full drift context
    # preserved in the audit row for the agent to diagnose.
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema validation
# MAGIC
# MAGIC With `failOnNewColumns` (Fix #2) Auto Loader already raises on drift. This
# MAGIC check is kept as a tripwire — if anyone later loosens `schemaEvolutionMode`
# MAGIC back to `rescue` or `addNewColumns`, the column-set check still catches it.

# COMMAND ----------

violations = validate_schema_columns([c for c in stream_df.columns if c != "_rescued_data"])
if violations:
    raise ValueError(f"Bronze schema validation failed: {violations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta
# MAGIC
# MAGIC Partition by `_ingest_date` to bound scan cost on incremental Silver reads.

# COMMAND ----------

query = (
    stream_df
    .withColumn("_ingest_date", F.current_date())
    .withColumn("_ingest_ts", F.current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "false")  # static schema — no silent additions
    .partitionBy("_ingest_date")
    .trigger(availableNow=True)
    .toTable(TABLE)
)
# 🔧 Fix #1 — block until the availableNow batch finishes so the audit step
# downstream reads the rows we just wrote, not a partial / empty snapshot.
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit
# MAGIC
# MAGIC Profile the batch we just wrote and append an audit record.

# COMMAND ----------

# This run's batch — scoped by RUN_START so drift detection and the profile
# only see rows committed by *this* invocation (Fix #1 + Fix #2 Option A).
this_run_batch = spark.table(TABLE).filter(F.col("_ingest_ts") >= F.lit(RUN_START))

# 🔧 Fix #2 (Option A) — detect drift via non-null `_rescued_data` in this run.
rescued = this_run_batch.filter(F.col("_rescued_data").isNotNull())
rescued_count = rescued.count()

# 🔧 Fix #5 — real anomalies wired through to the audit (no more hardcoded []).
anomalies: list[dict] = []
samples: list[str] = []
if rescued_count > 0:
    samples = [r["_rescued_data"] for r in rescued.select("_rescued_data").limit(5).collect()]
    anomalies.append(
        {
            "type": "schema_drift",
            "rescued_row_count": rescued_count,
            "examples": samples,
        }
    )

profile = profile_dataframe(this_run_batch)
status = "drift_detected" if rescued_count > 0 else "success"

write_audit_record(
    spark=spark,
    catalog=CATALOG,
    schema=AUDIT_SCHEMA,                # 🔧 Fix #8 — `ops` schema
    layer="bronze",
    job_run_id=RUN_ID,
    profile=profile,
    anomalies=anomalies,
    status=status,
)

print(f"Bronze ingest complete: {profile['row_count']} rows. status={status}")

# Audit-first-then-raise: the agent reads `_pipeline_audit` to diagnose
# failures, so the diagnostic context must be persisted before we kill the run.
if rescued_count > 0:
    raise ValueError(
        f"Schema drift detected: {rescued_count} rows in this run's batch landed "
        f"in _rescued_data. See {CATALOG}.{AUDIT_SCHEMA}._pipeline_audit "
        f"(run_id={RUN_ID}) for details. Examples: {samples}"
    )
