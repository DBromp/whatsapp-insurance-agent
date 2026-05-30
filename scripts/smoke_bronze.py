"""Local smoke test for the Bronze pipeline.

Runs the schema validation + profiling code path end-to-end against a small
synthetic parquet so we catch regressions before pushing to Databricks.
Does NOT exercise Delta or Unity Catalog — those are Databricks-only.

Run:
    make smoke
    # or:
    python scripts/smoke_bronze.py
"""

from __future__ import annotations

import pathlib
import subprocess
import sys


REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

TEST_PARQUET = REPO / "tests" / "fixtures" / "test_increment.parquet"


def _ensure_test_parquet() -> None:
    """Regenerate the test parquet if it doesn\'t exist."""
    if TEST_PARQUET.exists():
        return
    TEST_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    print(f"Generating {TEST_PARQUET}...")
    subprocess.run(
        [sys.executable, str(REPO / "scripts" / "generate_test_parquet.py"),
         "--rows", "100", "--out", str(TEST_PARQUET)],
        check=True,
    )


def main() -> int:
    _ensure_test_parquet()

    try:
        from pyspark.sql import SparkSession, functions as F
    except ImportError:
        print("✗ pyspark not installed — run `make install` first")
        return 1

    from src.profiling import profile_dataframe
    from src.schema import BRONZE_COLUMNS, build_bronze_schema, validate_schema_columns

    spark = (
        SparkSession.builder
        .appName("smoke-bronze")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        df = spark.read.schema(build_bronze_schema()).parquet(str(TEST_PARQUET))

        # 1. Schema validation
        violations = validate_schema_columns(df.columns)
        assert not violations, f"Schema validation failed: {violations}"
        print(f"✓ Schema validation passed ({len(BRONZE_COLUMNS)} columns)")

        # 2. Mirror the notebook\'s _ingest_date addition
        df_with_ingest = df.withColumn("_ingest_date", F.current_date())

        # 3. Profile
        profile = profile_dataframe(df_with_ingest)
        assert profile["row_count"] > 0, "Profile returned 0 rows"
        print(f"✓ Profile: {profile['row_count']} rows, {len(profile['columns'])} columns")
        print(f"  conversation_id null rate: {profile['null_rates'].get('conversation_id', 0):.3f}")
        print(f"  distinct campaign_ids: {profile['distinct_counts'].get('campaign_id', 0)}")

        # 4. Edge case — empty DataFrame must not raise (regression for the
        #    divide-by-zero fix in profiling.py)
        empty_df = spark.createDataFrame([], df.schema)
        empty_profile = profile_dataframe(empty_df, sample_n=10)
        assert empty_profile["row_count"] == 0
        print("✓ Empty-DataFrame profile returns zero shape without raising")

        print("\nSmoke test passed.")
        return 0
    except AssertionError as exc:
        print(f"✗ Smoke test failed: {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
