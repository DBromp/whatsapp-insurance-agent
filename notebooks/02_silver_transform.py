# Databricks notebook source
# Databricks notebook source
# MAGIC %pip install google-genai>=0.5.0 pydantic>=2.5.0 tenacity>=8.2.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Transformation
# MAGIC
# MAGIC Reads `nmstx_whatsapp_pipeline.bronze.messages`, masks PII, parses metadata,
# MAGIC rolls up conversations, and enriches them with LLM-extracted vehicle /
# MAGIC competitor / objection signals (hash-keyed cache per ADR-006).
# MAGIC
# MAGIC | Property | Value |
# MAGIC |---|---|
# MAGIC | Source | `nmstx_whatsapp_pipeline.bronze.messages` |
# MAGIC | Sinks | `silver.messages`, `silver.conversations`, `silver._extraction_cache`, `silver._pii_vault` |
# MAGIC | Run modes | `incremental` (default) / `full_refresh` (first run + prompt bumps) |
# MAGIC | Schedule | Workflow `silver_transform` runs every 15 min (`*/15 * * * *`) |
# MAGIC | Idempotent | Yes — every write is a Delta `MERGE` on a deterministic PK |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window
import hashlib
import sys

# Make `src/` importable when this runs as a Databricks notebook.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.insert(0, "/Workspace" + "/".join(notebook_path.split("/")[:-2]))

from src.gemini import ConversationInput, GeminiExtractor, body_hash, build_cache_rows
from src.pii import (
    mask_cep,
    mask_cpf,
    mask_email,
    mask_license_plate,
    mask_message_body,
    mask_phone,
    name_token_from_phone,
)
from src.profiling import profile_dataframe, write_audit_record

# 🔧 Fix #3 — explicit mode widget so cold first runs read all partitions.
dbutils.widgets.text("mode", "incremental")
RUN_MODE = dbutils.widgets.get("mode")
assert RUN_MODE in {"incremental", "full_refresh"}, f"invalid mode: {RUN_MODE}"

CATALOG = "nmstx_whatsapp_pipeline"
BRONZE_TABLE = f"{CATALOG}.bronze.messages"

SILVER_SCHEMA_NAME = "silver"
SILVER_MESSAGES = f"{CATALOG}.{SILVER_SCHEMA_NAME}.messages"
SILVER_CONVERSATIONS = f"{CATALOG}.{SILVER_SCHEMA_NAME}.conversations"
SILVER_CACHE = f"{CATALOG}.{SILVER_SCHEMA_NAME}._extraction_cache"
SILVER_PII_VAULT = f"{CATALOG}.{SILVER_SCHEMA_NAME}._pii_vault"

PII_SALT = dbutils.secrets.get(scope="nmstx-secrets", key="pii-salt")
GEMINI_API_KEY = dbutils.secrets.get(scope="nmstx-secrets", key="gemini-api-key")
PROMPT_VERSION = "v1"

EXTRACTION_BATCH_SIZE = 50  # ADR-006 — stay under free-tier RPM.

# 🔧 Fix #7 — length buckets per the data dictionary.
LENGTH_BUCKETS = [(4, "cold"), (10, "short"), (20, "medium")]  # else "long"

# 🔧 Fix #7 — RUN_ID disambiguation (mirrors Bronze notebook).
_job_run = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId()
if _job_run.isDefined():
    RUN_ID = str(_job_run.get())
else:
    RUN_ID = "local-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

print(f"mode={RUN_MODE}  run_id={RUN_ID}  prompt_version={PROMPT_VERSION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target tables exist
# MAGIC
# MAGIC Idempotent DDL — created on first run, no-ops thereafter. The cache and vault
# MAGIC tables must exist before the LEFT JOIN downstream returns sensible nulls.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_MESSAGES} (
    message_id           STRING NOT NULL,
    conversation_id      STRING NOT NULL,
    timestamp            TIMESTAMP NOT NULL,
    direction            STRING NOT NULL,
    message_type         STRING NOT NULL,
    message_body         STRING,
    status               STRING NOT NULL,
    channel              STRING NOT NULL,
    campaign_id          STRING NOT NULL,
    agent_id             STRING NOT NULL,
    conversation_outcome STRING NOT NULL,
    lead_phone_masked    STRING,
    lead_name_token      STRING,
    device               STRING,
    city                 STRING,
    state                STRING,
    response_time_sec    INT,
    is_business_hours    BOOLEAN,
    lead_source          STRING,
    is_lead              BOOLEAN,
    body_length          INT,
    _ingest_date         DATE,
    _silver_updated_at   TIMESTAMP
) USING DELTA
PARTITIONED BY (_ingest_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_CONVERSATIONS} (
    conversation_id        STRING NOT NULL,
    agent_id               STRING,
    campaign_id            STRING,
    lead_phone_masked      STRING,
    lead_name_token        STRING,
    outcome                STRING,
    n_messages             INT,
    n_inbound              INT,
    n_outbound             INT,
    first_ts               TIMESTAMP,
    last_ts                TIMESTAMP,
    duration_minutes       DOUBLE,
    length_bucket          STRING,
    state                  STRING,
    city                   STRING,
    lead_source            STRING,
    device                 STRING,
    vehicle_brand          STRING,
    vehicle_model          STRING,
    vehicle_year           INT,
    competitors_mentioned  ARRAY<STRING>,
    had_prior_sinistro     BOOLEAN,
    objection_category     STRING,
    _silver_updated_at     TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_CACHE} (
    conversation_id   STRING NOT NULL,
    body_hash         STRING NOT NULL,
    prompt_version    STRING NOT NULL,
    extracted_json    STRING NOT NULL,
    extracted_at      STRING NOT NULL
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PII_VAULT} (
    lead_name_token   STRING NOT NULL,
    phone_hash        STRING NOT NULL,
    first_seen        TIMESTAMP,
    last_seen         TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1 — Read Bronze
# MAGIC
# MAGIC `incremental` reads only today's partition; `full_refresh` reads everything.
# MAGIC Cold first run needs `mode=full_refresh` to pick up all 153k Bronze rows.

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)
if RUN_MODE == "incremental":
    bronze = bronze.filter(F.col("_ingest_date") == F.current_date())

print(f"Bronze rows in scope: {bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2 — Parse metadata JSON
# MAGIC
# MAGIC The `metadata` column is a JSON string. Expand it to typed columns so downstream
# MAGIC stages can filter and aggregate without re-parsing.

# COMMAND ----------

METADATA_SCHEMA = StructType([
    StructField("device", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("response_time_sec", IntegerType(), True),
    StructField("is_business_hours", BooleanType(), True),
    StructField("lead_source", StringType(), True),
])

bronze_parsed = (
    bronze
    .withColumn("_meta", F.from_json(F.col("metadata"), METADATA_SCHEMA))
    .select(
        "message_id",
        F.to_timestamp("timestamp").alias("timestamp"),
        "conversation_id",
        "direction",
        "sender_phone",
        "sender_name",
        "message_type",
        "message_body",
        "status",
        "channel",
        "campaign_id",
        "agent_id",
        "conversation_outcome",
        F.col("_meta.device").alias("device"),
        F.col("_meta.city").alias("city"),
        F.col("_meta.state").alias("state"),
        F.col("_meta.response_time_sec").alias("response_time_sec"),
        F.col("_meta.is_business_hours").alias("is_business_hours"),
        F.col("_meta.lead_source").alias("lead_source"),
        "_ingest_date",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3 — PII masking + vault write
# MAGIC
# MAGIC Lead identity is conversation-scoped — every message in a conversation gets
# MAGIC the SAME `lead_phone_masked` / `lead_name_token`, derived from the lead's
# MAGIC phone (the sender of the first inbound message). Outbound messages have a
# MAGIC different `sender_phone` (the agent), so we can't mask per-row.
# MAGIC
# MAGIC `sender_phone` and `sender_name` are dropped from the final output — only
# MAGIC the masked token columns persist. Vault gets a forensic `(token, phone_hash)`
# MAGIC row per distinct lead in this batch (Fix #5).

# COMMAND ----------

_SALT = PII_SALT  # captured locally for the UDF closure


@F.udf(returnType=StringType())
def udf_mask_body(body):
    if body is None:
        return None
    return mask_message_body(body, _SALT)


@F.udf(returnType=StringType())
def udf_mask_phone(phone):
    if phone is None:
        return None
    return mask_phone(phone, _SALT)


@F.udf(returnType=StringType())
def udf_name_token(phone):
    if phone is None:
        return None
    return name_token_from_phone(phone, _SALT)


@F.udf(returnType=StringType())
def udf_phone_hash(phone):
    """SHA-256 of the phone for the vault — never store the raw number."""
    if phone is None:
        return None
    return hashlib.sha256((phone + _SALT).encode()).hexdigest()


# One row per conversation: the lead's phone, picked from the first inbound message.
_first_inbound = (
    bronze_parsed
    .filter(F.col("direction") == F.lit("inbound"))
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("conversation_id").orderBy("timestamp")
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(F.col("conversation_id"), F.col("sender_phone").alias("lead_phone_raw"))
)

lead_identity = (
    _first_inbound
    .withColumn("lead_phone_masked", udf_mask_phone("lead_phone_raw"))
    .withColumn("lead_name_token", udf_name_token("lead_phone_raw"))
    .withColumn("_phone_hash", udf_phone_hash("lead_phone_raw"))
    .select("conversation_id", "lead_phone_masked", "lead_name_token", "_phone_hash")
)

# Broadcast the small per-conversation identity table back to every message.
silver_messages_raw = (
    bronze_parsed
    .join(F.broadcast(lead_identity), on="conversation_id", how="left")
    .withColumn("message_body", udf_mask_body("message_body"))
    .withColumn("is_lead", F.col("direction") == F.lit("inbound"))
    .withColumn("body_length", F.length(F.col("message_body")))
    .drop("sender_phone", "sender_name", "_phone_hash")
)

# 🔧 Fix #5 — vault MERGE per distinct lead in this batch.
vault_updates = (
    lead_identity
    .join(
        bronze_parsed.groupBy("conversation_id").agg(
            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen"),
        ),
        on="conversation_id",
        how="inner",
    )
    .filter(F.col("lead_name_token").isNotNull())
    .groupBy("lead_name_token")
    .agg(
        F.first("_phone_hash").alias("phone_hash"),
        F.min("first_seen").alias("first_seen"),
        F.max("last_seen").alias("last_seen"),
    )
)

vault_updates.createOrReplaceTempView("_vault_updates")
spark.sql(f"""
MERGE INTO {SILVER_PII_VAULT} AS t
USING _vault_updates AS s
ON t.lead_name_token = s.lead_name_token
WHEN MATCHED THEN UPDATE SET
    t.last_seen = GREATEST(t.last_seen, s.last_seen),
    t.first_seen = LEAST(t.first_seen, s.first_seen)
WHEN NOT MATCHED THEN INSERT (lead_name_token, phone_hash, first_seen, last_seen)
VALUES (s.lead_name_token, s.phone_hash, s.first_seen, s.last_seen)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4 — Filter audio rows out of LLM extraction
# MAGIC
# MAGIC Audio messages still land in `silver.messages` via the main flow (we don't
# MAGIC lose them); they're only excluded from the LLM enrichment because ASR
# MAGIC errors degrade extraction quality (Fix #8).

# COMMAND ----------

extraction_input = silver_messages_raw.filter(F.col("message_type") != F.lit("audio"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 5 — Hash each conversation's message bodies
# MAGIC
# MAGIC `body_hash = md5(concat(bodies in timestamp order))` per ADR-006 + Fix #2.

# COMMAND ----------

# Compute the per-conversation hash in driver-friendly form: groupBy collect_list,
# then a Python UDF that mirrors src.gemini.body_hash exactly.


@F.udf(returnType=StringType())
def udf_body_hash_struct(rows):
    """rows is a list of (timestamp, body) structs from collect_list."""
    if not rows:
        return None
    pairs = [(r["ts"], r["body"]) for r in rows if r["body"] is not None]
    return body_hash(pairs)


conv_hashes = (
    extraction_input
    .select(
        "conversation_id",
        F.struct(F.col("timestamp").cast("string").alias("ts"),
                 F.col("message_body").alias("body")).alias("pair"),
    )
    .groupBy("conversation_id")
    .agg(F.collect_list("pair").alias("pairs"))
    .withColumn("body_hash", udf_body_hash_struct("pairs"))
    .select("conversation_id", "body_hash")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 6 — Cache lookup
# MAGIC
# MAGIC LEFT JOIN against `silver._extraction_cache`. Rows where `extracted_json`
# MAGIC is null are misses and need a fresh Gemini call.

# COMMAND ----------

cache = spark.table(SILVER_CACHE).filter(F.col("prompt_version") == F.lit(PROMPT_VERSION))

conv_with_cache = (
    conv_hashes.alias("c")
    .join(
        cache.alias("k"),
        (F.col("c.conversation_id") == F.col("k.conversation_id"))
        & (F.col("c.body_hash") == F.col("k.body_hash")),
        "left",
    )
    .select(
        F.col("c.conversation_id"),
        F.col("c.body_hash"),
        F.col("k.extracted_json"),
    )
)

cache_misses = conv_with_cache.filter(F.col("extracted_json").isNull())
print(f"cache misses: {cache_misses.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 7 — Gemini call for cache misses (batched)
# MAGIC
# MAGIC Collect missing conversation_ids to the driver, batch into groups of
# MAGIC `EXTRACTION_BATCH_SIZE`, call Gemini, write fresh rows to
# MAGIC `silver._extraction_cache`. Per Fix #10: each completed batch is persisted
# MAGIC before moving on, so a mid-loop failure leaves successful work cached.

# COMMAND ----------

_misses = cache_misses.collect()
missing_ids = [r["conversation_id"] for r in _misses]
missing_hashes = {r["conversation_id"]: r["body_hash"] for r in _misses}

if missing_ids:
    # Pull the message bodies for the missing conversations only.
    bodies = (
        extraction_input
        .filter(F.col("conversation_id").isin(missing_ids))
        .select("conversation_id", "timestamp", "message_body")
        .orderBy("conversation_id", "timestamp")
        .collect()
    )

    grouped: dict[str, list[tuple[str, str]]] = {}
    for r in bodies:
        grouped.setdefault(r["conversation_id"], []).append(
            (str(r["timestamp"]), r["message_body"] or "")
        )

    extractor = GeminiExtractor(api_key=GEMINI_API_KEY, prompt_version=PROMPT_VERSION)

    n_batches = (len(missing_ids) + EXTRACTION_BATCH_SIZE - 1) // EXTRACTION_BATCH_SIZE
    print(f"running {n_batches} Gemini batch(es) over {len(missing_ids)} conversations")

    for i in range(0, len(missing_ids), EXTRACTION_BATCH_SIZE):
        batch_ids = missing_ids[i : i + EXTRACTION_BATCH_SIZE]
        batch_inputs = [
            ConversationInput(conversation_id=cid, messages=grouped[cid])
            for cid in batch_ids
        ]
        results = extractor.extract_batch(batch_inputs)
        rows = build_cache_rows(results, missing_hashes, PROMPT_VERSION)
        if rows:
            row_df = spark.createDataFrame([r.__dict__ for r in rows])
            row_df.createOrReplaceTempView("_cache_inserts")
            spark.sql(f"""
            MERGE INTO {SILVER_CACHE} AS t
            USING _cache_inserts AS s
            ON t.conversation_id = s.conversation_id
               AND t.body_hash = s.body_hash
               AND t.prompt_version = s.prompt_version
            WHEN NOT MATCHED THEN INSERT *
            """)
        print(f"  batch {i // EXTRACTION_BATCH_SIZE + 1}/{n_batches}: {len(rows)} rows cached")
else:
    print("no cache misses — all conversations served from cache")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 8 — Join extraction back to conversations

# COMMAND ----------

cache_fresh = spark.table(SILVER_CACHE).filter(F.col("prompt_version") == F.lit(PROMPT_VERSION))

EXTRACTION_SCHEMA = StructType([
    StructField("vehicle_brand", StringType(), True),
    StructField("vehicle_model", StringType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("competitors_mentioned", ArrayType(StringType()), True),
    StructField("had_prior_sinistro", BooleanType(), True),
    StructField("objection_category", StringType(), True),
])

extracted = (
    conv_hashes.alias("c")
    .join(
        cache_fresh.alias("k"),
        (F.col("c.conversation_id") == F.col("k.conversation_id"))
        & (F.col("c.body_hash") == F.col("k.body_hash")),
        "left",
    )
    .select(
        F.col("c.conversation_id"),
        F.from_json(F.col("k.extracted_json"), EXTRACTION_SCHEMA).alias("_ex"),
    )
    .select(
        "conversation_id",
        F.col("_ex.vehicle_brand").alias("vehicle_brand"),
        F.col("_ex.vehicle_model").alias("vehicle_model"),
        F.col("_ex.vehicle_year").alias("vehicle_year"),
        F.col("_ex.competitors_mentioned").alias("competitors_mentioned"),
        F.col("_ex.had_prior_sinistro").alias("had_prior_sinistro"),
        F.col("_ex.objection_category").alias("objection_category"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 9 — Build conversation rollup
# MAGIC
# MAGIC Aggregate messages → conversation-level facts, attach the extraction blob,
# MAGIC compute `length_bucket` from `n_messages`.

# COMMAND ----------

length_bucket_expr = (
    F.when(F.col("n_messages") <= LENGTH_BUCKETS[0][0], F.lit(LENGTH_BUCKETS[0][1]))
    .when(F.col("n_messages") <= LENGTH_BUCKETS[1][0], F.lit(LENGTH_BUCKETS[1][1]))
    .when(F.col("n_messages") <= LENGTH_BUCKETS[2][0], F.lit(LENGTH_BUCKETS[2][1]))
    .otherwise(F.lit("long"))
)

# Pick the first inbound message per conversation to source the per-conversation
# columns that vary by sender (state, city, lead_source, device).
# lead_phone_masked / lead_name_token are already broadcast-joined onto every
# message in Stage 3, so we just pick any row here.
lead_row = (
    silver_messages_raw
    .filter(F.col("is_lead"))
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("conversation_id").orderBy("timestamp")
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(
        "conversation_id",
        "lead_phone_masked",
        "lead_name_token",
        "state",
        "city",
        "lead_source",
        "device",
    )
)

agg = (
    silver_messages_raw
    .groupBy("conversation_id")
    .agg(
        F.first("agent_id", ignorenulls=True).alias("agent_id"),
        F.first("campaign_id", ignorenulls=True).alias("campaign_id"),
        F.first("conversation_outcome", ignorenulls=True).alias("outcome"),
        F.count(F.lit(1)).alias("n_messages"),
        F.sum(F.when(F.col("direction") == F.lit("inbound"), 1).otherwise(0)).alias("n_inbound"),
        F.sum(F.when(F.col("direction") == F.lit("outbound"), 1).otherwise(0)).alias("n_outbound"),
        F.min("timestamp").alias("first_ts"),
        F.max("timestamp").alias("last_ts"),
    )
    .withColumn(
        "duration_minutes",
        (F.unix_timestamp("last_ts") - F.unix_timestamp("first_ts")) / 60.0,
    )
    .withColumn("length_bucket", length_bucket_expr)
)

silver_conv_full = (
    agg.alias("a")
    .join(lead_row.alias("l"), "conversation_id", "left")
    .join(extracted.alias("e"), "conversation_id", "left")
    .select(
        "a.conversation_id",
        "a.agent_id",
        "a.campaign_id",
        "l.lead_phone_masked",
        "l.lead_name_token",
        "a.outcome",
        "a.n_messages",
        "a.n_inbound",
        "a.n_outbound",
        "a.first_ts",
        "a.last_ts",
        "a.duration_minutes",
        "a.length_bucket",
        "l.state",
        "l.city",
        "l.lead_source",
        "l.device",
        "e.vehicle_brand",
        "e.vehicle_model",
        "e.vehicle_year",
        "e.competitors_mentioned",
        "e.had_prior_sinistro",
        "e.objection_category",
    )
    .withColumn("_silver_updated_at", F.current_timestamp())   # 🔧 Fix #6
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 10 — MERGE into `silver.messages`
# MAGIC
# MAGIC Keyed on `message_id`. `_silver_updated_at` is set on every inserted / updated
# MAGIC row so Day 3 Gold can read incrementally (Fix #6).

# COMMAND ----------

silver_messages_final = (
    silver_messages_raw
    .select(
        "message_id",
        "conversation_id",
        "timestamp",
        "direction",
        "message_type",
        "message_body",
        "status",
        "channel",
        "campaign_id",
        "agent_id",
        "conversation_outcome",
        "lead_phone_masked",
        "lead_name_token",
        "device",
        "city",
        "state",
        "response_time_sec",
        "is_business_hours",
        "lead_source",
        "is_lead",
        "body_length",
        "_ingest_date",
    )
    .withColumn("_silver_updated_at", F.current_timestamp())
)

silver_messages_final.createOrReplaceTempView("_silver_messages_src")
spark.sql(f"""
MERGE INTO {SILVER_MESSAGES} AS t
USING _silver_messages_src AS s
ON t.message_id = s.message_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 11 — MERGE into `silver.conversations`

# COMMAND ----------

silver_conv_full.createOrReplaceTempView("_silver_conv_src")
spark.sql(f"""
MERGE INTO {SILVER_CONVERSATIONS} AS t
USING _silver_conv_src AS s
ON t.conversation_id = s.conversation_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 12 — Profile + audit
# MAGIC
# MAGIC Same `_pipeline_audit` table Bronze writes into. Layer set to `"silver"`.

# COMMAND ----------

profile_target = (
    spark.table(SILVER_MESSAGES)
    .filter(F.col("_silver_updated_at") >= F.current_date())
)
profile = profile_dataframe(profile_target, sample_n=20_000)

# TODO(day-4 agent): compare profile against previous Silver run's baseline.
anomalies: list[dict] = []

write_audit_record(
    spark=spark,
    catalog=CATALOG,
    schema="ops",                          # 🔧 Fix #8 — shared ops audit table.
    layer="silver",
    job_run_id=RUN_ID,
    profile=profile,
    anomalies=anomalies,
    status="success",
)

print(
    f"Silver run complete: "
    f"{profile['row_count']} rows updated in silver.messages this run."
)
