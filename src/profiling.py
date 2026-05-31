"""Reusable Bronze/Silver profiling utilities.

Used by the data engineer during development and by the supervising agent at runtime
to detect anomalies in incoming data (sudden null spikes, new enum values, distribution
drift, etc.).

PySpark is imported lazily inside the functions that need it so that pure-Python
helpers (`detect_anomalies`) can be imported in environments without pyspark.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def profile_dataframe(df: "DataFrame", sample_n: int | None = None) -> dict[str, Any]:
    """One-pass profile of a Spark DataFrame.

    Returns shape + null rates + (approximate) distinct counts in a single ``agg``
    over the input. Trades exact distinct counts for HyperLogLog approximation
    (default ``rsd=0.05``) so the call stays cheap on multi-million-row
    Silver/Gold tables.

    The previous implementation triggered ~1 + 2N full scans (one ``count`` plus
    a ``filter().count()`` and a ``distinct().count()`` per column). At Bronze
    scale (153k rows) that's harmless; at Gold scale it bites.

    ``sample_n`` is accepted for backward compatibility but currently ignored —
    ``approx_count_distinct`` is fast enough on full tables that sampling adds
    complexity without proportional benefit. Re-add as a deterministic Bernoulli
    sample if a future caller needs it.

    Args:
        df: Spark DataFrame to profile.
        sample_n: Ignored (kept for caller compatibility).

    Returns:
        Dict with keys: ``row_count``, ``columns``, ``null_rates``,
        ``distinct_counts``. Empty DataFrames return a zero-shape dict so the
        agent's downstream comparisons keep working without special casing.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    columns = df.columns

    # Build per-column agg expressions in a single pass.
    agg_exprs = [F.count(F.lit(1)).alias("__row_count__")]
    for c in columns:
        is_string = isinstance(df.schema[c].dataType, StringType)
        if is_string:
            null_cond = F.col(c).isNull() | (F.col(c) == "")
        else:
            null_cond = F.col(c).isNull()
        agg_exprs.append(F.sum(F.when(null_cond, 1).otherwise(0)).alias(f"__null_{c}__"))
        agg_exprs.append(F.approx_count_distinct(F.col(c), rsd=0.05).alias(f"__dc_{c}__"))

    row = df.agg(*agg_exprs).collect()[0]
    row_count = row["__row_count__"]

    if row_count == 0:
        return {
            "row_count": 0,
            "columns": columns,
            "null_rates": {c: 0.0 for c in columns},
            "distinct_counts": {c: 0 for c in columns},
        }

    null_rates = {c: row[f"__null_{c}__"] / row_count for c in columns}
    distinct_counts = {c: row[f"__dc_{c}__"] for c in columns}

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

    Pure Python — no pyspark dependency.
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
    from pyspark.sql import functions as F

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
