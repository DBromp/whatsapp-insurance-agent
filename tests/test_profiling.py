"""Tests for src/profiling.py.

Tests that need Spark take the `spark` fixture and will be skipped if pyspark
isn\'t installed. Tests that exercise pure-Python helpers (detect_anomalies)
have no Spark dependency.
"""

from __future__ import annotations

import pytest

from src.profiling import detect_anomalies


# ---- Pure-Python tests (always run) ---------------------------------------


def test_detect_anomalies_flags_null_rate_spike():
    """Baseline 0% nulls -> current 30% nulls in one column should produce one anomaly."""
    baseline = {
        "columns": ["id", "name"],
        "null_rates": {"id": 0.0, "name": 0.0},
        "distinct_counts": {"id": 100, "name": 100},
    }
    current = {
        "columns": ["id", "name"],
        "null_rates": {"id": 0.0, "name": 0.3},
        "distinct_counts": {"id": 100, "name": 100},
    }

    anomalies = detect_anomalies(current, baseline, null_rate_tolerance=0.10)

    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["type"] == "null_rate_drift"
    assert a["column"] == "name"
    assert a["baseline"] == 0.0
    assert a["current"] == 0.3


def test_detect_anomalies_returns_empty_when_baseline_matches_current():
    """No drift -> no anomalies."""
    profile = {
        "columns": ["id"],
        "null_rates": {"id": 0.0},
        "distinct_counts": {"id": 100},
    }
    assert detect_anomalies(profile, profile) == []


def test_detect_anomalies_flags_missing_column():
    """Column present in baseline but not current."""
    baseline = {
        "columns": ["id", "name"],
        "null_rates": {"id": 0.0, "name": 0.0},
        "distinct_counts": {"id": 100, "name": 100},
    }
    current = {
        "columns": ["id"],
        "null_rates": {"id": 0.0},
        "distinct_counts": {"id": 100},
    }

    anomalies = detect_anomalies(current, baseline)

    assert any(a["type"] == "missing_column" and a["column"] == "name" for a in anomalies)


# ---- Spark-dependent tests (skipped when pyspark missing) -----------------


def test_profile_empty_dataframe_does_not_divide_by_zero(spark):
    """Regression for the divide-by-zero bug: empty DataFrame + sample_n must not raise."""
    from src.profiling import profile_dataframe

    df = spark.createDataFrame([], "id STRING, name STRING")

    result = profile_dataframe(df, sample_n=10)

    assert result["row_count"] == 0
    assert set(result["columns"]) == {"id", "name"}
    assert all(rate == 0.0 for rate in result["null_rates"].values())
    assert all(count == 0 for count in result["distinct_counts"].values())


def test_profile_returns_expected_shape(spark):
    """Shape check on a known 5-row DataFrame with mixed nulls."""
    from src.profiling import profile_dataframe

    rows = [
        ("a", "x"),
        ("b", None),
        ("c", "y"),
        ("d", ""),
        ("e", "z"),
    ]
    df = spark.createDataFrame(rows, "id STRING, name STRING")

    result = profile_dataframe(df)

    assert result["row_count"] == 5
    assert set(result["columns"]) == {"id", "name"}
    assert result["null_rates"]["id"] == 0.0
    assert result["null_rates"]["name"] == pytest.approx(0.4)
    assert result["distinct_counts"]["id"] == 5
