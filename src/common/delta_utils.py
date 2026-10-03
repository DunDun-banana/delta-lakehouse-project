"""Small Delta Lake helpers shared by the pipeline, audit and tests.

PySpark and delta-spark are imported lazily so that modules which only parse
``_delta_log`` JSON (and their unit tests) can import this file cheaply.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

AUTO_MERGE_CONF = "spark.databricks.delta.schema.autoMerge.enabled"


def latest_commit(spark: "SparkSession", path: str) -> dict[str, Any] | None:
    """Return the newest commit of a Delta table, or None if it is not a table."""

    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    rows = DeltaTable.forPath(spark, path).history(1).collect()
    if not rows:
        return None
    row = rows[0].asDict(recursive=True)
    return {
        "version": int(row["version"]),
        "timestamp": str(row["timestamp"]),
        "operation": row.get("operation"),
        "operationParameters": row.get("operationParameters") or {},
        "operationMetrics": row.get("operationMetrics") or {},
    }


@contextmanager
def schema_auto_merge(spark: "SparkSession") -> Iterator[None]:
    """Enable additive schema evolution for the MERGEs inside the block.

    delta-spark 2.4 has no ``DeltaMergeBuilder.withSchemaEvolution()``; MERGE
    only adds new source columns when this session flag is true. Scoping it
    keeps every other write in the session strict, and the previous value is
    restored (or the flag unset) even if the MERGE fails.
    """

    previous = spark.conf.get(AUTO_MERGE_CONF, None)
    spark.conf.set(AUTO_MERGE_CONF, "true")
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(AUTO_MERGE_CONF)
        else:
            spark.conf.set(AUTO_MERGE_CONF, previous)
