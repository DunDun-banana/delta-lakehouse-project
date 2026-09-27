"""Task 3 - Time Travel & Audit.

TODO:
- inspect table history
- restore previous version if needed
- validate audit trail
"""

from __future__ import annotations
import argparse
import json
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def latest_commit(spark: SparkSession, path: str) -> dict | None:
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    rows = DeltaTable.forPath(spark, path).history(1).collect()
    if not rows:
        return None
    row = rows[0].asDict(recursive=True)
    return {"version": int(row["version"]), "timestamp": str(row["timestamp"]),
            "operation": row.get("operation"), "operationMetrics": row.get("operationMetrics") or {},
            "userMetadata": row.get("userMetadata")}


def read_version(spark: SparkSession, path: str, version: int):
    if version < 0:
        raise ValueError("version must be >= 0")
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def history(spark: SparkSession, path: str, limit: int = 20):
    return DeltaTable.forPath(spark, path).history(limit)


def restore_version(spark: SparkSession, path: str, version: int):
    """Explicit manual operation; creates a NEW version, does not erase history."""
    if version < 0:
        raise ValueError("version must be >= 0")
    DeltaTable.forPath(spark, path).restoreToVersion(version)
    return latest_commit(spark, path)


def main():
    from src.silver.silver_pipeline import make_spark, ROOT
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default=str(ROOT / "data/silver/taxi_trips"))
    parser.add_argument("--history", type=int, default=20)
    parser.add_argument("--version", type=int)
    parser.add_argument("--trip-id")
    parser.add_argument("--restore", type=int, help="DANGEROUS: explicitly restore entire Silver table")
    args = parser.parse_args()
    spark = make_spark("local[2]")
    try:
        history(spark, args.silver, args.history).show(truncate=False)
        if args.version is not None:
            frame = read_version(spark, args.silver, args.version)
            if args.trip_id:
                frame = frame.filter(frame.trip_id == args.trip_id)
            frame.show(20, truncate=False)
        if args.restore is not None:
            print("RESTORE COMMIT", json.dumps(restore_version(spark, args.silver, args.restore), default=str))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
