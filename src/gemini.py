"""Gemini extraction client — batched, structured output, rate-limit aware.

The client is intentionally "dumb": it makes one Gemini call per batch of
conversations and returns typed Pydantic results. The Silver notebook owns
cache lookups, persistence to ``silver._extraction_cache``, and batching
schedule. Keeping the LLM client cache-agnostic makes it easy to swap models
later (Groq fallback per ADR-002) without leaking cache concerns.

See:
- ADR-002 (model choice) and ADR-006 (hash cache) in ``docs/decisions.md``
- ``docs/plans/02-day2-silver.md`` (File 3 section)
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from google import genai
from google.genai import types as genai_types
from google.genai.errors import ClientError, ServerError
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Structured output schemas
# ---------------------------------------------------------------------------


ObjectionCategory = Literal["price", "coverage", "trust", "timing", "none"]


class ExtractionResult(BaseModel):
    """Per-conversation extraction shape (Silver-level, coarse).

    Day 3 Gold re-classifies ``objection_category`` into a 7-bucket taxonomy
    (price / coverage_gaps / trust_credibility / bad_timing /
    existing_insurer_satisfaction / vehicle_age_mismatch / other) using the
    same cached extraction blob — no fresh Gemini call needed.
    """

    vehicle_brand: str | None = Field(
        default=None,
        description="Lower-case brand name as written (e.g. 'honda', 'fiat'). null if unknown.",
    )
    vehicle_model: str | None = Field(
        default=None,
        description="Lower-case model (e.g. 'civic', 'gol'). null if unknown.",
    )
    vehicle_year: int | None = Field(
        default=None,
        description="4-digit year. null if unknown.",
    )
    competitors_mentioned: list[str] = Field(
        default_factory=list,
        description=(
            "Distinct competitor brand names mentioned by either side. "
            "Pick from: porto seguro, azul seguros, bradesco seguros, sulamérica, "
            "liberty, allianz, tokio marine, mapfre, hdi."
        ),
    )
    had_prior_sinistro: bool = Field(
        default=False,
        description="True if the lead mentioned a previous claim/accident (sinistro).",
    )
    objection_category: ObjectionCategory = Field(
        default="none",
        description="Coarse Silver-level objection signal; refined into 7 buckets in Gold.",
    )


class _ConversationExtraction(BaseModel):
    """Internal wrapper — one element of the batched response."""

    conversation_id: str
    extraction: ExtractionResult


class _BatchExtractionResponse(BaseModel):
    """The shape Gemini returns when handed a batch."""

    results: list[_ConversationExtraction]


# ---------------------------------------------------------------------------
# Input shape (what the notebook hands to the extractor)
# ---------------------------------------------------------------------------


@dataclass
class ConversationInput:
    """One conversation's messages, ordered or unordered.

    Attributes:
        conversation_id: Stable id from Bronze.
        messages: List of ``(timestamp_iso, body)`` tuples. The extractor
            sorts by timestamp before serializing into the prompt so the LLM
            sees the dialogue in causal order.
    """

    conversation_id: str
    messages: list[tuple[str, str]]


# ---------------------------------------------------------------------------
# Hash helper (kept in this module so callers import one thing)
# ---------------------------------------------------------------------------


def body_hash(messages: list[tuple[str, str]]) -> str:
    """MD5 over message bodies in **timestamp** order (not alphabetical).

    ``messages`` is the same shape as ``ConversationInput.messages`` —
    ``(timestamp_iso, body)`` tuples. Sorting by timestamp means re-ordering
    a conversation invalidates the cache (as it should — re-ordered messages
    can mean a different extraction).

    Matches ADR-006 spec.
    """
    ordered = [body for _, body in sorted(messages, key=lambda m: m[0])]
    return hashlib.md5("\n".join(ordered).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = (
    "You are an extraction assistant for a Brazilian auto-insurance sales pipeline. "
    "Input is a batch of WhatsApp conversations between insurance agents and leads, "
    "written in Brazilian Portuguese. For each conversation, extract structured "
    "fields: vehicle data (brand, model, year), competitor brands mentioned by either "
    "side, whether the lead mentioned a previous claim (sinistro), and the dominant "
    "objection category if any.\n\n"
    "Rules:\n"
    "- Use null for unknown vehicle fields (do not guess).\n"
    "- Brand and model names are lower-cased.\n"
    "- Competitor list is restricted to the canonical set in the schema description.\n"
    "- objection_category is one of: price, coverage, trust, timing, none.\n"
    "- Return one result per conversation, preserving input order, with the same "
    "conversation_id."
)


class GeminiExtractor:
    """Calls Gemini 2.5 Flash with structured output for a batch of conversations."""

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-2.5-flash",
        prompt_version: str = "v1",
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.prompt_version = prompt_version
        self._client = genai.Client(api_key=api_key)

    # ---- public API ------------------------------------------------------

    def extract_batch(
        self, conversations: list[ConversationInput]
    ) -> list[_ConversationExtraction]:
        """Extract structured signals for a batch of up to ~50 conversations.

        Returns one ``_ConversationExtraction`` per input conversation. If the
        model returns fewer results than requested (truncation, partial
        response), the partial list is returned — the caller (Silver notebook)
        persists the partials to ``_extraction_cache`` and re-queues the
        missing ones on the next iteration. Idempotent under failure.

        Raises ``ClientError`` / ``ServerError`` on persistent API failure
        after retries are exhausted (tenacity decorator on the inner call).
        """
        if not conversations:
            return []
        prompt = self._build_prompt(conversations)
        raw = self._call_gemini(prompt)
        parsed = _BatchExtractionResponse.model_validate_json(raw)
        return parsed.results

    # ---- internals -------------------------------------------------------

    def _build_prompt(self, conversations: list[ConversationInput]) -> str:
        """Serialize a batch of conversations into a single Gemini prompt."""
        parts: list[str] = [
            "Extract structured fields for each conversation below.",
            "Return JSON matching the response schema with one result per conversation.",
            "",
        ]
        for c in conversations:
            ordered = sorted(c.messages, key=lambda m: m[0])
            parts.append(f"--- conversation_id={c.conversation_id} ---")
            for ts, body in ordered:
                parts.append(f"[{ts}] {body}")
            parts.append("")
        return "\n".join(parts)

    @retry(
        stop=stop_after_delay(120),
        wait=wait_exponential(min=5, max=60),
        retry=retry_if_exception_type((ClientError, ServerError)),
        reraise=True,
    )
    def _call_gemini(self, prompt: str) -> str:
        """Single Gemini API call with retry budget.

        Retry policy: up to ~2 minutes of total retry wall-clock with
        exponential backoff (5s → 60s caps). Catches the SDK's typed
        ``ClientError`` / ``ServerError`` (covers rate-limit and 5xx); other
        exceptions raise immediately.
        """
        response = self._client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=_SYSTEM_PROMPT,
                response_mime_type="application/json",
                response_schema=_BatchExtractionResponse,
                temperature=0.1,
            ),
        )
        # `response.text` is the JSON string when response_mime_type is JSON.
        return response.text


# ---------------------------------------------------------------------------
# Cache row helper
# ---------------------------------------------------------------------------


@dataclass
class CacheRow:
    """One row to be inserted into ``silver._extraction_cache``.

    Schema (Delta table): ``(conversation_id, body_hash, prompt_version,
    extracted_json, extracted_at)``.
    """

    conversation_id: str
    body_hash: str
    prompt_version: str
    extracted_json: str
    extracted_at: str


def build_cache_rows(
    results: list[_ConversationExtraction],
    hashes_by_conv: dict[str, str],
    prompt_version: str,
) -> list[CacheRow]:
    """Turn a batch result + per-conversation body hashes into cache rows.

    Args:
        results: What ``GeminiExtractor.extract_batch`` returned.
        hashes_by_conv: ``{conversation_id: body_hash}`` computed by the notebook.
        prompt_version: Matches ``GeminiExtractor.prompt_version`` at call time.

    Returns:
        One CacheRow per result. Caller writes them to the cache via a Delta
        ``MERGE`` keyed on ``(conversation_id, body_hash, prompt_version)``.

    Skips any result whose conversation_id is missing from ``hashes_by_conv``
    (defensive — shouldn't happen, but if it does we'd rather drop the row
    than write a corrupt cache entry).
    """
    now = datetime.now(timezone.utc).isoformat()
    rows: list[CacheRow] = []
    for r in results:
        h = hashes_by_conv.get(r.conversation_id)
        if h is None:
            continue
        rows.append(
            CacheRow(
                conversation_id=r.conversation_id,
                body_hash=h,
                prompt_version=prompt_version,
                extracted_json=r.extraction.model_dump_json(),
                extracted_at=now,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Convenience accessor for environment-based init (local smoke testing)
# ---------------------------------------------------------------------------


def from_env(prompt_version: str = "v1") -> GeminiExtractor:
    """Build a ``GeminiExtractor`` from ``GOOGLE_API_KEY`` in the environment.

    For local smoke testing only — on Databricks the notebook pulls the key
    from a secret scope and passes it explicitly.
    """
    key = os.environ.get("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("GOOGLE_API_KEY is not set in the environment.")
    return GeminiExtractor(api_key=key, prompt_version=prompt_version)
