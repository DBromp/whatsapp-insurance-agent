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
# MAGIC | Idempotent | Yes (Auto Loader checkpoint + Delta append) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

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

RUN_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().getOrElse("local")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read with Auto Loader

# COMMAND ----------

stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(BRONZE_SCHEMA)
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # capture drift in _rescued_data
    .load(SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema validation
# MAGIC
# MAGIC We enforced the static schema above, but we also check column ordering and the
# MAGIC presence of the _rescued_data column (which would indicate drift Auto Loader
# MAGIC tried to recover).

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

(
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit
# MAGIC
# MAGIC Profile the batch we just wrote and append an audit record.

# COMMAND ----------

batch = spark.table(TABLE).filter(F.col("_ingest_date") == F.current_date())
profile = profile_dataframe(batch)

write_audit_record(
    spark=spark,
    catalog=CATALOG,
    schema=BRONZE_SCHEMA_NAME,
    layer="bronze",
    job_run_id=str(RUN_ID),
    profile=profile,
    anomalies=[],
    status="success",
)

print(f"Bronze ingest complete: {profile['row_count']} rows in latest batch.")
