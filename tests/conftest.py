"""Shared pytest fixtures: one small local Spark session for the whole run."""

from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture(scope="session")
def spark():
    # Small, fast settings; must be set before the JVM starts.
    os.environ["SPARK_DRIVER_MEMORY"] = "1g"
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = "2"
    # Keep Spark scratch files out of data/tmp (SPARK_LOCAL_DIRS overrides
    # spark.local.dir in local mode).
    scratch = tempfile.TemporaryDirectory(prefix="pytest-spark-")
    os.environ["SPARK_LOCAL_DIRS"] = scratch.name

    from src.common.spark import create_spark

    session = create_spark("pytest", "local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    scratch.cleanup()
