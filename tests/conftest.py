"""Shared pytest fixtures.

The Spark fixture is session-scoped so a single JVM serves the whole test run.
Tests that don\'t take the `spark` fixture run without needing pyspark at all,
so test_schema.py stays fast and dependency-free.
"""

from __future__ import annotations

import pytest

try:
    from pyspark.sql import SparkSession  # noqa: F401  (imported for availability check)
    _HAS_PYSPARK = True
except ImportError:
    _HAS_PYSPARK = False


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit tests. Skips the test if pyspark isn\'t installed."""
    if not _HAS_PYSPARK:
        pytest.skip("pyspark not installed in this environment")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("whatsapp-insurance-agent-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
