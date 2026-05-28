# Databricks setup walkthrough

Manual steps to perform once per workspace. Automate later if the project ever leaves single-developer scope.

## 1. Create the catalog and schemas

In the Databricks UI → Catalog Explorer:

```sql
CREATE CATALOG IF NOT EXISTS nmstx_whatsapp_pipeline;
USE CATALOG nmstx_whatsapp_pipeline;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

## 2. Create the raw-files Volume

```sql
CREATE VOLUME IF NOT EXISTS nmstx_whatsapp_pipeline.bronze.raw_files;
```

Then upload `conversations_bronze.parquet` to:
```
/Volumes/nmstx_whatsapp_pipeline/bronze/raw_files/conversations/
```

via the Catalog Explorer → Volumes → Upload to this volume.

## 3. Create the secret scope

Use the Databricks CLI:

```bash
databricks secrets create-scope nmstx-secrets
databricks secrets put-secret nmstx-secrets gemini-api-key
# paste your Gemini key when prompted, then Ctrl+D

databricks secrets put-secret nmstx-secrets pii-salt
# paste a 32-byte random hex string (generate with: python -c "import secrets; print(secrets.token_hex(32))")
```

## 4. Connect the GitHub repo

In Databricks → Repos → Add Repo → paste the GitHub URL of `whatsapp-insurance-agent`.

## 5. Create Workflows

After Day 1 code is pushed:
- Workflow `bronze_ingest` → `notebooks/01_bronze_ingest.py`, no schedule yet (trigger manually for now)
- Workflow `silver_transform` → `notebooks/02_silver_transform.py`, schedule `*/15 * * * *` once Day 2 lands
- Workflow `gold_refresh` → `notebooks/03_gold_refresh.py`, triggered after `silver_transform` succeeds
- Workflow `agent_supervisor` → `notebooks/04_agent_supervisor.py`, continuous (Day 4)
