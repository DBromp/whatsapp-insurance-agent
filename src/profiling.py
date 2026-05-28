"""Reusable Bronze/Silver profiling utilities.

Used by the data engineer during development and by the supervising agent at runtime
to detect anomalies in incoming data (sudden null spikes, new enum values, distribution
drift, etc.).
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType


def profile_dataframe(df: DataFrame, sample_n: int | None = None) -> dict[str, Any]:
    """Return a profile dict with shape, null rates, distinct counts, and basic stats.

    Args:
        df: Spark DataFrame to profile.
        sample_n: Optional row sample size for distinct-count computation.

    Returns:
        Dict with keys: row_count, columns, null_rates, distinct_counts.
    """
    sample = df.sample(fraction=min(1.0, sample_n / df.count())) if sample_n else df

    row_count = df.count()
    columns = sample.columns

    def _null_or_empty(col_name: str):
        cond = F.col(col_name).isNull()
        if isinstance(sample.schema[col_name].dataType, StringType):
            cond = cond | (F.col(col_name) == "")
        return cond

    null_rates = {
        c: sample.filter(_null_or_empty(c)).count() / max(sample.count(), 1)
        for c in columns
    }

    distinct_counts = {c: sample.select(c).distinct().count() for c in columns}

    return {
        "row_count": row_count,
        "columns": columns,
        "null_rates": null_rates,
        "distinct_counts": distinct_counts,
    }


def detect_anomalies(
    current: dict[str, Any],
    baseline: dict[str, Any],
    null_rate_tolerance: float = 0.10,
    distinct_count_tolerance: float = 0.50,
) -> list[dict[str, Any]]:
    """Compare a current profile against a baseline. Return list of anomalies.

    An anomaly is raised when:
      - null_rate for a column drifts by more than `null_rate_tolerance` (absolute)
      - distinct_count drifts by more than `distinct_count_tolerance` (relative)
      - a column is missing or unexpectedly added
    """
    anomalies: list[dict[str, Any]] = []

    base_cols = set(baseline.get("columns", []))
    curr_cols = set(current.get("columns", []))

    for missing in base_cols - curr_cols:
        anomalies.append({"type": "missing_column", "column": missing})
    for extra in curr_cols - base_cols:
        anomalies.append({"type": "unexpected_column", "column": extra})

    for col in base_cols & curr_cols:
        base_null = baseline["null_rates"].get(col, 0.0)
        curr_null = current["null_rates"].get(col, 0.0)
        if abs(curr_null - base_null) > null_rate_tolerance:
            anomalies.append(
                {
                    "type": "null_rate_drift",
                    "column": col,
                    "baseline": round(base_null, 4),
                    "current": round(curr_null, 4),
                }
            )

        base_dc = baseline["distinct_counts"].get(col, 0)
        curr_dc = current["distinct_counts"].get(col, 0)
        if base_dc > 0:
            rel_change = abs(curr_dc - base_dc) / base_dc
            if rel_change > distinct_count_tolerance:
                anomalies.append(
                    {
                        "type": "distinct_count_drift",
                        "column": col,
                        "baseline": base_dc,
                        "current": curr_dc,
                        "relative_change": round(rel_change, 4),
                    }
                )

    return anomalies


def write_audit_record(
    spark,
    catalog: str,
    schema: str,
    layer: str,
    job_run_id: str,
    profile: dict[str, Any],
    anomalies: list[dict[str, Any]],
    status: str,
) -> None:
    """Persist an audit record so the agent can read it back during incident triage."""
    record = [
        (
            job_run_id,
            layer,
            status,
            json.dumps(profile, default=str),
            json.dumps(anomalies, default=str),
        )
    ]
    df = spark.createDataFrame(
        record,
        "run_id STRING, layer STRING, status STRING, profile_json STRING, anomalies_json STRING",
    ).withColumn("created_at", F.current_timestamp())

    df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}._pipeline_audit")
