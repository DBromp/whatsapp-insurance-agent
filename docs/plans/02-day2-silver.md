**Status:** Approved · **Owner:** TBD

---

# Day 2 — Silver Layer + PII Masking + Gemini Extraction (patched)

> **Note for VS Code Claude.** This is the original Day 2 plan with 12 review fixes applied. Each fix is annotated inline as a `🔧 Fix #N` callout right where the change lands, so you can see both what changed and why. A summary of all 12 fixes is at the top. Treat this file as the new source of truth for Day 2 work.

## Summary of changes vs. original

| # | Fix | Where it lands |
|---|---|---|
| 1 | Renamed `mask_name(phone, salt)` → `name_token_from_phone(phone, salt)` — the original signature implied it took a name, but it takes a phone. Silent footgun. | File 1 public API + build-order table |
| 2 | `body_hash` orders messages by timestamp, not alphabetical sort. Alphabetical sort would mask real semantic re-ordering. | File 3 hash helper |
| 3 | Added explicit `mode` widget (`incremental` vs `full_refresh`). Without it, the first run would skip yesterday's 153k Bronze rows. | File 4 Stage 1 + new "Run modes" section |
| 4 | Called out the two-stage objection refinement (Silver = 5-value coarse signal; Gold = 7-bucket taxonomy on Day 3). Avoids re-extraction on Day 3. | File 3 Pydantic model section |
| 5 | Added `silver._pii_vault` write step in File 4 Stage 3. The skill specifies the vault but the original plan never said where rows get written. | File 4 Stage 3 |
| 6 | Added `_silver_updated_at` watermark column on every MERGE so Day 3 Gold can do incremental reads. | File 4 Stages 8–10 |
| 7 | Moved `length_bucket` from "open decision" into File 4 with the dictionary thresholds (cold ≤ 4, short 5–10, medium 11–20, long 21+). Original said `cold ≤ 2` which contradicts the dictionary. | File 4 Stage 7 |
| 8 | Made the audio-transcript handling explicit: filter `message_type = "audio"` out of LLM extraction. Single decision rather than open question. | File 3 + File 4 Stage 6 |
| 9 | Retry policy widened: `stop_after_delay(120)` + catch Gemini's typed rate-limit exception. Three attempts in 30s is not enough to survive an RPD cap hit. | File 3 retry section |
| 10 | Defined partial-batch failure behaviour: write successful rows to cache first, then raise. Retries don't redo successful work. | File 3 `extract_batch` contract |
| 11 | Specified `_deterministic_digits` algorithm: per-byte modulo over the hexdigest. Original "sliced into n decimal digits" was ambiguous. | File 1 helpers section |
| 12 | Cold-run time estimate corrected from ~5 hours to ~25 min (15k convs / 50 batch × ~5s = ~25 min under free-tier). Also flagged the ADR-006 update. | File 4 verification + ADR-006 note |

---

## Context

Day 1 shipped Bronze end-to-end: 153,228 messages live in `nmstx_whatsapp_pipeline.bronze.messages` with strict schema enforcement and an audit trail. Day 2 builds the Silver layer per `references/transformations.md`, `docs/decisions.md` ADR-006, and the task breakdown in [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) §Day 2 (tasks 2.1–2.8): cleaned + PII-masked messages, conversation rollups with LLM-extracted vehicle/competitor/objection signals, a hash-keyed extraction cache so we don\'t blow the Gemini free tier on every refresh, idempotency tests, and a scheduled Databricks Workflow.

**Day 2 Definition of Done (per PROJECT_PLAN.md):** `silver.messages` and `silver.conversations` populated. Zero PII visible in either table. LLM extraction cached so reruns are cheap. Silver job runs on a 15-min schedule.

Include unit tests in `tests/test_pii.py` covering the cases listed under File 2.

## Sequence

Six checkpoints in dependency order. Each is a stop-and-review point. Subsequent steps only start after Daniel signs off on the previous one. The numbering matches PROJECT_PLAN.md tasks 2.1–2.8.

1. `src/pii.py` — Brazilian PII masking. Pure Python. *(task 2.1)*
2. `tests/test_pii.py` — pytest assertions, ~30 cases per PROJECT_PLAN.md. *(task 2.2)*
3. `src/gemini.py` — Gemini extraction client + Pydantic models + hash helper. *(task 2.4)*
4. `notebooks/02_silver_transform.py` — PySpark transform combining 2.3, 2.5, 2.6 in one notebook. *(tasks 2.3 + 2.5 + 2.6)*
5. Idempotency check — re-run Silver, confirm zero net new rows (MERGE invariant). *(task 2.7)*
6. Wire `silver_transform` Databricks Job + cron schedule `*/15 * * * *`. *(task 2.8)*

---

## File 1: `src/pii.py`

**Public API (one function per Brazilian PII pattern):**

```python
def mask_phone(phone: str, salt: str) -> str: ...
def name_token_from_phone(phone: str, salt: str) -> str: ...  # 🔧 Fix #1
def mask_cpf(cpf: str, salt: str) -> str: ...
def mask_cep(cep: str, salt: str) -> str: ...
def mask_email(email: str, salt: str) -> str: ...
def mask_license_plate(plate: str, salt: str) -> str: ...
def mask_message_body(body: str, salt: str) -> str: ...   # orchestrates all regexes
```

> **🔧 Fix #1 — rename `mask_name` to `name_token_from_phone`**
> The original `mask_name(phone: str, salt: str)` was a footgun: the name and signature implied it took a name, but the canonical PII rule keys the lead\'s display token off their phone (see `references/pii_masking.md`). Callers writing `mask_name(some_lead.sender_name, salt)` would silently get a non-deterministic token. New name makes the contract obvious at the call site.

**Internal helpers (module-private, leading underscore):**

- `_deterministic_digits(seed: str, salt: str, n: int) -> str` — see Fix #11 below for the exact algorithm. Backbone of every numeric mask.
- `_normalize_phone(phone: str) -> str` — strips parens / spaces / dashes, ensures `+55` prefix.

> **🔧 Fix #11 — `_deterministic_digits` algorithm is now specified**
> Original said "SHA-256(seed + salt) sliced into n decimal digits" — ambiguous (per-char `% 10` vs single big-int `% (10**n)`). Spec: compute `hashlib.sha256((seed + salt).encode()).hexdigest()`, walk it in 2-char chunks (each chunk = `int(pair, 16)` ∈ [0, 255]), take `% 10` to get one decimal digit, repeat until you have `n` digits. Uniform-enough distribution and stable across Python versions.

**Module-level regex constants** (compile once, reuse):

- `PHONE_RE`, `CPF_RE`, `CEP_RE`, `EMAIL_RE`, `PLATE_MERCOSUL_RE`, `PLATE_OLD_RE`

**Conventions (mirror Day 1):**

- `from __future__ import annotations` at the top.
- Google-style docstrings matching `src/profiling.py:17`.
- No new dependencies — `re` and `hashlib` are stdlib.

**Build order** (simplest → trickiest, foundation first):

| # | Function | Why this order |
|---|---|---|
| 1 | `_deterministic_digits` | Foundation — every numeric mask depends on it |
| 2 | `mask_phone` | Direct application of helper #1 |
| 3 | `name_token_from_phone` | Different output shape (hex token via `[:8]` slice) |
| 4 | `mask_cep` | Branching on whether hyphen is present |
| 5 | `mask_cpf` | Multiple format variants, normalization step |
| 6 | `mask_email` | `str.split("@")`, simple slicing |
| 7 | `mask_license_plate` | Detects Mercosul vs old plate, branches accordingly |
| 8 | `mask_message_body` | Orchestrates the regexes; integration step |

---

## File 2: `tests/test_pii.py`

**Coverage shape** — per PROJECT_PLAN.md §2.2, **~30 cases including malformed/edge inputs, idempotency, and dimension preservation**. For each masker:

- **Determinism / idempotency** — same input + same salt → same output across calls; calling mask twice on already-masked input is stable.
- **Dimension preservation** — `len(masked) == len(original)`; format markers (`+`, `-`, `.`, `@`) in identical positions.
- **No-passthrough** — masked output ≠ original (sanity check; use realistic inputs unlikely to collide on the hash).
- **Malformed inputs** — `mask_phone("11988734012")` (no `+55`), `mask_cpf("12345678900")` (no dots), `mask_cep("12345678")` (no hyphen), `mask_license_plate("ABC-1234")` (old format) — should normalize or branch correctly.
- **Edge cases** — empty string, whitespace-only, already-masked input, mixed PII in one `message_body`.

Mirror `tests/test_schema.py` style: plain functions, no `unittest.TestCase`, no fixtures. Use `SALT = "test-salt-" + "x" * 54` (64 chars, mirrors production token_hex(32)) as a module constant.

---

## File 3: `src/gemini.py`

**Public API:**

```python
class ExtractionResult(BaseModel):
    vehicle_brand: str | None
    vehicle_model: str | None
    vehicle_year: int | None
    competitors_mentioned: list[str]
    had_prior_sinistro: bool
    objection_category: Literal["price", "coverage", "trust", "timing", "none"]
    # 🔧 Fix #4 — Silver-level coarse signal only.
    # Gold refines this into the 7-bucket taxonomy on Day 3
    # (price / coverage_gaps / trust_credibility / bad_timing /
    #  existing_insurer_satisfaction / vehicle_age_mismatch / other)
    # using the same cached extraction blob — no re-extract needed.

class GeminiExtractor:
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash", prompt_version: str = "v1"): ...
    def extract_batch(self, conversations: list[ConversationInput]) -> list[ExtractionResult]: ...
```

> **🔧 Fix #4 — explicit two-stage objection refinement**
> Silver\'s `ExtractionResult.objection_category` is a 5-value Literal (matches `references/transformations.md`). Gold\'s `gold.objection_taxonomy` from `references/gold_insights.md` has 7 richer categories. That\'s intentional refinement, not duplication. The Silver extraction blob (cached in `silver._extraction_cache`) carries enough context that Day 3 Gold can re-classify into the 7-bucket taxonomy without a fresh Gemini call. Document this so Day 3 doesn\'t accidentally re-extract.

**`ConversationInput`** is a dataclass with `(conversation_id, messages: list[tuple[str, str]])` — each tuple is `(timestamp, body)`. The extractor uses the timestamps to order messages before serializing into the prompt. Audio messages (see Fix #8) are filtered upstream in File 4, so the extractor itself doesn\'t need a message_type field.

**Caching contract** — Gemini calls are batched (~50 conversations), but cache lookup happens upstream in the Silver notebook (see File 4). `GeminiExtractor` is "dumb" — it doesn\'t know about the cache; it just calls the API and returns typed results.

**Retry policy** via `tenacity`:

```python
from tenacity import retry, stop_after_delay, wait_exponential, retry_if_exception_type
from google.genai.errors import ClientError, ServerError  # typed exceptions

@retry(
    stop=stop_after_delay(120),                # 🔧 Fix #9: was stop_after_attempt(3); too short to survive a free-tier RPD cap hit
    wait=wait_exponential(min=5, max=60),
    retry=retry_if_exception_type((ClientError, ServerError)),
    reraise=True,
)
def _call_gemini(...): ...
```

> **🔧 Fix #9 — wider retry window and typed exception catching**
> Original `stop_after_attempt(3)` with `wait_exponential(min=2, max=30)` budgets at most ~60s of retries. A real Gemini free-tier rate-limit response stays rate-limited for several minutes. New policy gives 2 min of total retry budget and catches the SDK\'s typed `ClientError`/`ServerError` rather than raw HTTP codes (the `google-genai` SDK doesn\'t expose HTTP status directly at the Python layer).

**Partial-batch failure contract** (Fix #10): if Gemini returns N < batch_size results before failing, `extract_batch` writes the N successful results to the cache **before** re-raising. Callers retry with the smaller remaining set. Cache hits the second time around mean retries don\'t redo successful work.

> **🔧 Fix #10 — partial-batch failure is now defined**
> Original plan didn\'t say what happened if Gemini returned 47/50 results. The defined behaviour: write the 47 successes to `_extraction_cache` first, then raise. The Silver notebook\'s next iteration sees 47 hits + 3 misses and only re-calls Gemini for the 3 remaining. Idempotent under failure.

**Hash helper** in same module for proximity:

```python
def body_hash(messages: list[tuple[str, str]]) -> str:
    """md5 over message bodies in TIMESTAMP order (not alphabetical).

    messages: list of (timestamp_iso, body) tuples — same shape as
    ConversationInput.messages. Sort by timestamp before hashing so that
    semantic re-ordering of a conversation invalidates the cache.

    Matches ADR-006 spec.
    """
    ordered = [body for _, body in sorted(messages, key=lambda m: m[0])]
    return hashlib.md5("\n".join(ordered).encode()).hexdigest()
```

> **🔧 Fix #2 — hash by timestamp order, not alphabetical**
> Original was `md5(concat(sorted(bodies)))`. Sorting bodies alphabetically would make a re-ordered conversation hash identically — but a re-ordered conversation actually means something different to the LLM, so the cache should be invalidated. Bronze rows are deterministic by timestamp already, so this is just dropping the alphabetical `sorted()` and ordering by timestamp instead.

**Resolved defaults** (no longer "open decisions"):

- `extracted_at` timezone — UTC via `datetime.now(timezone.utc).isoformat()`.
- `extracted_json` cache column shape — single JSON string (matches transformations.md "extracted_json" naming); per-field columns added later if needed for SQL filtering.
- Audio handling — see Fix #8 below.

> **🔧 Fix #8 — audio transcripts are filtered out of LLM extraction**
> Audio messages carry ASR errors that degrade extraction quality. Decision: `notebooks/02_silver_transform.py` filters `message_type = "audio"` rows out of the LLM extraction stream **before** computing `body_hash`. The audio rows still land in `silver.messages` (we don\'t lose them); they just don\'t participate in conversation-level enrichment. Rationale: audio is <3% of message volume per the Day 1 profile, so the loss is small and the cleanliness gain is large. If audio share grows materially later, revisit by adding an `extraction_confidence` column.

---

## File 4: `notebooks/02_silver_transform.py`

**Reuses Day 1 verbatim (per reuse map):**

- `sys.path` bootstrap pattern from [01_bronze_ingest.py:27-28](notebooks/01_bronze_ingest.py#L27-L28)
- `RUN_ID` capture from [01_bronze_ingest.py:42-43](notebooks/01_bronze_ingest.py#L42-L43)
- `profile_dataframe()` + `write_audit_record()` with `layer="silver"`
- `CATALOG = "nmstx_whatsapp_pipeline"`, partition by `_ingest_date`

**Run modes** (Fix #3 — new section):

```python
dbutils.widgets.text("mode", "incremental")  # 🔧 Fix #3
RUN_MODE = dbutils.widgets.get("mode")
assert RUN_MODE in {"incremental", "full_refresh"}
```

- `incremental` (default for scheduled runs): read Bronze filtered to `_ingest_date = current_date()`.
- `full_refresh` (used for the first run and after a `PROMPT_VERSION` bump): read all Bronze partitions.

> **🔧 Fix #3 — explicit run mode**
> Original Stage 1 said "filter to current `_ingest_date` for incremental work." On first run, all 153k Bronze rows live under yesterday\'s `_ingest_date` and would be silently skipped. The widget defaults to `incremental` for scheduled runs but lets us pass `mode=full_refresh` on the first execution and any time we bump `PROMPT_VERSION` (which should re-extract everything). Documented in Verification step 1.

**New constants:**

```python
SILVER_SCHEMA_NAME = "silver"
SILVER_MESSAGES = f"{CATALOG}.{SILVER_SCHEMA_NAME}.messages"
SILVER_CONVERSATIONS = f"{CATALOG}.{SILVER_SCHEMA_NAME}.conversations"
SILVER_CACHE = f"{CATALOG}.{SILVER_SCHEMA_NAME}._extraction_cache"
SILVER_PII_VAULT = f"{CATALOG}.{SILVER_SCHEMA_NAME}._pii_vault"
PII_SALT = dbutils.secrets.get(scope="nmstx-secrets", key="pii-salt")
GEMINI_API_KEY = dbutils.secrets.get(scope="nmstx-secrets", key="gemini-api-key")
PROMPT_VERSION = "v1"

# 🔧 Fix #7 — length buckets from the data dictionary, materialised here
LENGTH_BUCKETS = [
    (4,  "cold"),    # 2–4 messages
    (10, "short"),   # 5–10
    (20, "medium"),  # 11–20
    # else "long"    # 21+
]
```

> **🔧 Fix #7 — `length_bucket` thresholds match the dictionary**
> Original had `cold ≤ 2`, which contradicts the data dictionary (which says cold/bounce = 2–4 messages). Corrected to `cold ≤ 4 / short 5–10 / medium 11–20 / long 21+`. Materialised as a constant in File 4 (where it\'s used) rather than left as an "open decision" in File 3.

**Pipeline stages** (each its own notebook cell, so failures are localized for the supervisor):

1. **Read Bronze** — filtered per `RUN_MODE` (see above).
2. **Parse metadata JSON** — `F.from_json(F.col("metadata"), metadata_schema)` then `select(... "metadata.*")`.
3. **PII mask + vault write** — register `mask_message_body`, `mask_phone`, `name_token_from_phone` as pandas_udfs. Drop `sender_phone`/`sender_name`; keep `lead_phone_masked`/`lead_name_token`.
   **Vault write (Fix #5):** for every distinct lead in this batch, MERGE `(lead_name_token, sha256(sender_phone))` into `silver._pii_vault`. This is the forensic reverse map; never exposed in Gold, strict ACLs.

> **🔧 Fix #5 — `silver._pii_vault` gets populated here**
> Original plan masked names but never said where the reverse map lived. The vault row is one MERGE per distinct lead per batch: `(token, original_hash, first_seen, last_seen)`. The hash side stores `sha256(sender_phone + PII_SALT)` — never the raw phone — so even the vault doesn\'t hold reversible PII.

4. **Filter out audio rows for extraction** (Fix #8): `extraction_input = silver_messages.filter(F.col("message_type") != "audio")`. The audio rows continue to land in `silver.messages` via the main flow; they just don\'t feed extraction.

5. **Compute `body_hash`** per conversation — groupBy `conversation_id`, collect `(timestamp, message_body)` tuples in timestamp order, call `body_hash()` via pandas_udf. (Per Fix #2, ordering is timestamp-based.)

6. **Cache lookup** — `LEFT JOIN` against `silver._extraction_cache` on `(conversation_id, body_hash, prompt_version)`.

7. **Gemini call for cache misses** — `.filter(F.col("extracted_json").isNull())` → collect to driver in batches of 50 → `GeminiExtractor.extract_batch()` → write fresh rows to `_extraction_cache` (Fix #10: successful rows persist even if the batch raises). After this, compute `length_bucket` from `n_messages` using the table in the constants block (Fix #7).

8. **Join extraction back** — full conversations table now has `extracted_json` populated.

9. **MERGE INTO `silver.messages`** on `message_id`. **Add `_silver_updated_at = F.current_timestamp()` on every merged row** (Fix #6).

10. **MERGE INTO `silver.conversations`** on `conversation_id`. **Add `_silver_updated_at = F.current_timestamp()`** (Fix #6).

11. **Profile + audit** — `profile_dataframe(silver_messages_today)` → `write_audit_record(..., layer="silver")`.

> **🔧 Fix #6 — `_silver_updated_at` watermark column**
> Per `references/transformations.md`, Day 3 Gold tables refresh incrementally by reading rows where `_silver_updated_at > <last_gold_watermark>`. The original plan never wrote this column. Now every MERGE sets `_silver_updated_at = current_timestamp()` on inserted **and** updated rows. Tiny addition, invisible-failure-prone if forgotten.

**Idempotency invariant** (ADR / transformations.md): every write is `MERGE` keyed on a deterministic PK. No raw `.append()`.

---

## Checkpoint 5: Idempotency test (task 2.7)

After the Silver notebook succeeds once, **re-run it immediately** with no Bronze changes. Verify:

- `silver.messages` row count unchanged.
- `silver.conversations` row count unchanged.
- `silver._extraction_cache` row count unchanged (zero Gemini calls — all cache hits).
- `silver._pii_vault` row count unchanged.
- Second run completes in **under one minute** (vs ~25 min for the cold first run — Fix #12).

> **🔧 Fix #12 — cold-run time estimate corrected**
> Original plan and ADR-006 quoted "~5 hours" for the cold first run. That was a placeholder written before the batching design landed. Real number: 15k convs / 50 per batch = 300 Gemini calls × ~5s each ≈ 25 min wall-clock. Free-tier RPD (1500) is comfortably above 300. Update ADR-006 to reflect the corrected estimate so Day 1 documentation matches Day 2 reality.

If row counts grow, the MERGE keys are wrong — investigate before proceeding.

---

## Checkpoint 6: Schedule the Workflow (task 2.8)

In Databricks UI:

- **Workflows → Create job** named `silver_transform`
- **Task:** Notebook → `notebooks/02_silver_transform.py`, serverless compute
- **Parameter:** `mode = incremental` (the `full_refresh` mode is only used manually on first run / prompt bumps)
- **Schedule:** Cron `*/15 * * * *` (every 15 minutes)
- **Notification on failure** (optional but recommended) — so the supervising agent\'s eventual escalation path has somewhere to land

After saving, manually trigger once to confirm the scheduled config works. Then leave it running so Day 3\'s Gold layer has continuously-fresh Silver to read from.

---

## Critical files & references

| File | Why |
|---|---|
| [src/schema.py](src/schema.py) | Convention source (enums, validator pattern) |
| [src/profiling.py](src/profiling.py) | Reuse `profile_dataframe()`, `write_audit_record()` verbatim |
| [notebooks/01_bronze_ingest.py](notebooks/01_bronze_ingest.py) | sys.path bootstrap, RUN_ID capture, audit pattern |
| [tests/test_schema.py](tests/test_schema.py) | Test style (plain assert, no TestCase) |
| [docs/decisions.md](docs/decisions.md) | ADR-005 (fail-fast Bronze), ADR-006 (hash cache — update cold-run estimate per Fix #12) |
| [.claude/skills/whatsapp-insurance-pipeline/references/transformations.md](.claude/skills/whatsapp-insurance-pipeline/references/transformations.md) | Silver table shapes (incl. `_extraction_cache`) |
| [.claude/skills/whatsapp-insurance-pipeline/references/pii_masking.md](.claude/skills/whatsapp-insurance-pipeline/references/pii_masking.md) | PII patterns + masked-format invariants |
| [.claude/skills/whatsapp-insurance-pipeline/references/schema.md](.claude/skills/whatsapp-insurance-pipeline/references/schema.md) | Bronze column types + metadata JSON shape |
| [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) §Day 2 | Master task list (2.1–2.8), estimates, Day 2 DoD |
| `requirements.txt` | Day 2 uses pre-declared `google-genai`, `pydantic`, `tenacity` only — no new deps |

---

## Verification

**File 1 (`pii.py`) — local:**

```bash
python -c "from src.pii import mask_phone, mask_cpf; print(mask_phone(\"+5511988734012\", \"salt\"), mask_cpf(\"123.456.789-00\", \"salt\"))"
# expect: +55XXXXXXXXXXX same length, CPF preserves dots/dash
```

**File 2 (`test_pii.py`):**

```bash
make test    # or: pytest tests/test_pii.py -v
```

All tests green; determinism + dimension invariants pass.

**File 3 (`gemini.py`) — local smoke (requires GOOGLE_API_KEY in `.env`):**

```bash
python -c "from src.gemini import GeminiExtractor, body_hash; print(body_hash([(\"2026-02-01T10:00\", \"hello\"), (\"2026-02-01T10:01\", \"world\")]))"
```

Hash deterministic; client instantiates without error. (Skip actual API call locally to avoid quota burn.)

**File 4 (Silver notebook) — Databricks:**

1. Push, pull in Databricks Repo, create Workflow `silver_transform` targeting `notebooks/02_silver_transform.py`.
2. **First run** with `mode=full_refresh` (cold cache) — expect ~25 min wall-clock (per Fix #12); 50-conversation batches; verify rows land in `silver._extraction_cache` and `silver._pii_vault`.
3. **Second run** with `mode=incremental` (warm cache, no new data) — expect sub-minute completion; zero Gemini calls; `silver.messages` and `silver.conversations` unchanged in row count.
4. **SQL spot-checks:**
   - `SELECT COUNT(*) FROM silver.messages` ≈ 153,228 (matches Bronze, since dedupe is on `message_id` which is already unique)
   - `SELECT COUNT(*) FROM silver.conversations` = 15,000
   - `SELECT * FROM silver.messages LIMIT 5` — `sender_phone` column absent; `lead_phone_masked` present, `+55` preserved, 11 trailing chars masked
   - `SELECT extracted_json FROM silver._extraction_cache LIMIT 3` — non-null structured JSON
   - `SELECT COUNT(*) FROM silver._pii_vault` — equals distinct lead count (~14,918)
   - `SELECT MAX(_silver_updated_at) FROM silver.messages` — recent timestamp (Fix #6 sanity check)
   - `SELECT layer, status, created_at FROM bronze._pipeline_audit ORDER BY created_at DESC LIMIT 5` — silver run logged
5. **Failure-injection check (optional)** — change `PROMPT_VERSION` to `"v2"`, re-run with `mode=full_refresh`, observe full re-extract (cache miss across the board).

**Schedule (Checkpoint 6) — Databricks Workflow:**

1. Workflow `silver_transform` exists with `*/15 * * * *` cron.
2. Manually triggered once after save — succeeds.
3. After ~15 min idle, next scheduled run fires automatically (verify in run history).

**Day 2 acceptance (per PROJECT_PLAN.md DoD):**

- ✓ `silver.messages` and `silver.conversations` populated.
- ✓ Zero PII visible in either table (manual `SELECT lead_phone_masked, lead_name_token FROM silver.messages LIMIT 20` spot-check).
- ✓ LLM extraction cached (warm rerun under a minute, zero Gemini calls).
- ✓ Silver job runs on schedule (workflow has next-run timestamp).
- ✓ Audio rows present in `silver.messages` but absent from `silver._extraction_cache` (Fix #8 sanity check).
- ✓ `_silver_updated_at` watermark column populated on every row (Fix #6).
