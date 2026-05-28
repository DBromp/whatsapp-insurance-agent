"""Bronze layer schema definitions and validation.

The Bronze schema is intentionally strict: 14 columns in a fixed order with known
types. We fail loudly on drift so the supervising agent can detect upstream changes
and either auto-patch or escalate. See docs/decisions.md ADR-005 for rationale.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

BRONZE_COLUMNS = (
    "message_id",
    "conversation_id",
    "timestamp",
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
    "metadata",
)


def build_bronze_schema():
    """Build the PySpark StructType for the Bronze table. Lazy import keeps tests light."""
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("message_id", StringType(), nullable=False),
            StructField("conversation_id", StringType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
            StructField("direction", StringType(), nullable=False),
            StructField("sender_phone", StringType(), nullable=False),
            StructField("sender_name", StringType(), nullable=True),
            StructField("message_type", StringType(), nullable=False),
            StructField("message_body", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("channel", StringType(), nullable=False),
            StructField("campaign_id", StringType(), nullable=False),
            StructField("agent_id", StringType(), nullable=False),
            StructField("conversation_outcome", StringType(), nullable=False),
            StructField("metadata", StringType(), nullable=True),
        ]
    )


DIRECTIONS = frozenset({"inbound", "outbound"})

MESSAGE_TYPES = frozenset(
    {"text", "audio", "image", "document", "sticker", "contact", "video", "location"}
)

STATUSES = frozenset({"sent", "delivered", "read", "failed"})

CHANNELS = frozenset({"whatsapp"})

OUTCOMES = frozenset(
    {
        "venda_fechada",
        "perdido_preco",
        "perdido_concorrente",
        "ghosting",
        "desistencia_lead",
        "proposta_enviada",
        "em_negociacao",
    }
)

TERMINAL_OUTCOMES = OUTCOMES - {"em_negociacao"}


def validate_schema_columns(actual):
    """Compare actual columns against expected. Empty list = valid."""
    expected = set(BRONZE_COLUMNS)
    actual_set = set(actual)
    violations = []

    missing = expected - actual_set
    if missing:
        violations.append("Missing columns: " + str(sorted(missing)))

    extra = actual_set - expected
    if extra:
        violations.append("Unexpected columns: " + str(sorted(extra)))

    n = len(BRONZE_COLUMNS)
    actual_prefix = list(actual)[:n]
    expected_list = list(BRONZE_COLUMNS)
    if actual_prefix != expected_list:
        violations.append("Column order differs from expected BRONZE_COLUMNS")

    return violations


def validate_enum_values(df, column, allowed):
    """Return list of unexpected values in `column`. Empty = valid."""
    distinct = {row[column] for row in df.select(column).distinct().collect()}
    unexpected = distinct - allowed - {None}
    return sorted(unexpected)
