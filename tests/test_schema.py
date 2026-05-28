"""Tests for Bronze schema constants and validation helpers."""

import pytest

from src.schema import (
    BRONZE_COLUMNS,
    DIRECTIONS,
    MESSAGE_TYPES,
    OUTCOMES,
    STATUSES,
    TERMINAL_OUTCOMES,
    validate_schema_columns,
)


def test_bronze_has_exactly_14_columns():
    assert len(BRONZE_COLUMNS) == 14


def test_bronze_columns_are_unique():
    assert len(set(BRONZE_COLUMNS)) == len(BRONZE_COLUMNS)


def test_message_types_includes_real_data_extras():
    """Real data contains contact/video/location even though they're not in the dictionary."""
    assert {"contact", "video", "location"} <= MESSAGE_TYPES


def test_directions_are_exactly_two():
    assert DIRECTIONS == {"inbound", "outbound"}


def test_outcomes_contain_all_seven():
    assert len(OUTCOMES) == 7
    assert "em_negociacao" in OUTCOMES


def test_terminal_outcomes_exclude_em_negociacao():
    assert "em_negociacao" not in TERMINAL_OUTCOMES
    assert "venda_fechada" in TERMINAL_OUTCOMES


def test_validate_columns_passes_on_exact_match():
    violations = validate_schema_columns(list(BRONZE_COLUMNS))
    assert violations == []


def test_validate_columns_detects_missing():
    truncated = list(BRONZE_COLUMNS)[:-1]
    violations = validate_schema_columns(truncated)
    assert any("Missing" in v for v in violations)


def test_validate_columns_detects_extra():
    extended = list(BRONZE_COLUMNS) + ["surprise_column"]
    violations = validate_schema_columns(extended)
    assert any("Unexpected" in v for v in violations)


def test_validate_columns_detects_reorder():
    reordered = list(BRONZE_COLUMNS[::-1])
    violations = validate_schema_columns(reordered)
    assert any("order" in v.lower() for v in violations)


def test_statuses_includes_failed_even_though_absent_in_snapshot():
    """Real-data snapshot has no `failed` rows, but the schema must accept them."""
    assert "failed" in STATUSES
