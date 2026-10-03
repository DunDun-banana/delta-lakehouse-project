"""Task 2 - incremental Bronze -> Silver with Delta streaming and foreachBatch MERGE.

Bronze is read as a Delta stream; every microbatch is validated, deduplicated
and MERGEd into Silver, rejected rows go to a quarantine table, and one audit
row per microbatch is upserted into ``batch_audit``. The checkpoint records
which Bronze commits are done, so re-running only processes new commits.

Run from the project root:
    python -m src.silver.silver_pipeline --once
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    BATCH_AUDIT_PATH,
    BRONZE_PATH,
    REJECTED_PATH,
    SILVER_CHECKPOINT,
    SILVER_PATH,
)
from src.common.delta_utils import latest_commit
from src.silver.silver_cleaning import deduplicate, validate_and_split
from src.silver.silver_merge import merge_rejected, merge_silver

LOG = logging.getLogger("silver")

# The existing checkpoint is bound to this query name; do not rename it.
QUERY_NAME = "silver_taxi_cdc"


def _version(commit: dict | None) -> int | None:
    return commit["version"] if commit else None


def _audit_row(
    spark: SparkSession,
    batch_id: int,
    counts: dict[str, int],
    before: dict | None,
    after: dict | None,
) -> DataFrame:
    """Build the one-row audit DataFrame (column names and order are fixed).

    Built from literals with ``spark.range(1)`` so no Python worker is started.
    """

    return spark.range(1).select(
        F.lit(int(batch_id)).cast("long").alias("batch_id"),
        F.lit(counts["incoming"]).cast("long").alias("incoming"),
        F.lit(counts["valid"]).cast("long").alias("valid"),
        F.lit(counts["rejected"]).cast("long").alias("rejected"),
        F.lit(counts["winners"]).cast("long").alias("winners"),
        F.current_timestamp().alias("processed_at"),
        F.lit(_version(before)).cast("long").alias("silver_version_before"),
        F.lit(_version(after)).cast("long").alias("silver_version_after"),
        F.lit(after["operation"] if after else None).cast("string").alias("silver_operation"),
        F.lit(json.dumps(after["operationMetrics"] if after else {})).alias(
            "silver_operation_metrics"
        ),
        F.lit(after["timestamp"] if after else None).cast("string").alias(
            "silver_commit_timestamp"
        ),
    )


def _upsert_audit(spark: SparkSession, audit: DataFrame, audit_path: str) -> None:
    """Insert or replace the audit row of one microbatch (keyed by batch_id)."""

    if not DeltaTable.isDeltaTable(spark, audit_path):
        audit.write.format("delta").mode("errorifexists").save(audit_path)
        return
    (
        DeltaTable.forPath(spark, audit_path)
        .alias("t")
        .merge(audit.alias("s"), "t.batch_id = s.batch_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(
    spark: SparkSession,
    df: DataFrame,
    batch_id: int,
    silver: str,
    rejected_path: str,
    audit_path: str,
) -> None:
    """foreachBatch handler.

    Spark may replay a microbatch after a crash, so every write is an
    idempotent MERGE. The three tables are three separate Delta commits, not
    one cross-table transaction; batch_audit is a summary, not a lock.
    """

    if df.isEmpty():
        return
    df = df.persist()
    winners = None
    try:
        valid, rejected = validate_and_split(df)
        # Cache only the raw microbatch and the winners to limit local spill.
        winners = deduplicate(valid).persist()

        incoming = df.count()
        valid_count = valid.count()
        counts = {
            "incoming": incoming,
            "valid": valid_count,
            "rejected": incoming - valid_count,
            "winners": winners.count(),
        }
        before = latest_commit(spark, silver)
        if counts["winners"]:
            merge_silver(spark, winners, silver)
        after = latest_commit(spark, silver)
        if counts["rejected"]:
            merge_rejected(spark, rejected, rejected_path)

        _upsert_audit(spark, _audit_row(spark, batch_id, counts, before, after), audit_path)
        LOG.info(
            json.dumps(
                {
                    "event": "silver_batch",
                    "batch_id": batch_id,
                    "silver_version_before": _version(before),
                    "silver_version_after": _version(after),
                    "silver_operation": after["operation"] if after else None,
                    **counts,
                }
            )
        )
    finally:
        if winners is not None:
            winners.unpersist()
        df.unpersist()


def run_silver(
    spark: SparkSession,
    bronze: str = str(BRONZE_PATH),
    silver: str = str(SILVER_PATH),
    rejected: str = str(REJECTED_PATH),
    audit: str = str(BATCH_AUDIT_PATH),
    checkpoint: str = str(SILVER_CHECKPOINT),
    continuous: bool = False,
    interval: str = "60 seconds",
    max_bytes_per_trigger: str = "128m",
) -> dict:
    """Process new Bronze commits into Silver and return a summary dict.

    By default uses ``availableNow``: every pending Bronze commit is processed
    in size-bounded microbatches, then the query stops. ``continuous=True``
    keeps polling Bronze every ``interval`` and only returns on failure.
    """

    if not DeltaTable.isDeltaTable(spark, bronze):
        raise FileNotFoundError("Bronze Delta table missing: " + bronze)
    for path in (silver, rejected, audit, checkpoint):
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    before = latest_commit(spark, silver)
    # maxBytesPerTrigger is the Delta source option (a soft limit per file),
    # not the spark.sql.streaming.* session setting.
    stream = (
        spark.readStream.format("delta")
        .option("maxBytesPerTrigger", max_bytes_per_trigger)
        .load(bronze)
    )
    writer = (
        stream.writeStream.queryName(QUERY_NAME)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(
            lambda batch, batch_id: process_batch(spark, batch, batch_id, silver, rejected, audit)
        )
    )
    if continuous:
        writer = writer.trigger(processingTime=interval)
    else:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()
    if query.exception():
        raise RuntimeError(str(query.exception()))

    after = latest_commit(spark, silver)
    summary = {
        "event": "silver_finished",
        "silver_path": silver,
        "silver_version_before": _version(before),
        "silver_version_after": _version(after),
        "microbatches": sum(1 for p in query.recentProgress if p.get("numInputRows", 0) > 0),
    }
    LOG.info(json.dumps(summary))
    return summary


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--bronze", default=str(BRONZE_PATH))
    parser.add_argument("--silver", default=str(SILVER_PATH))
    parser.add_argument("--rejected", default=str(REJECTED_PATH))
    parser.add_argument("--audit", default=str(BATCH_AUDIT_PATH))
    parser.add_argument("--checkpoint", default=str(SILVER_CHECKPOINT))
    parser.add_argument("--master", default="local[*]")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once", action="store_true", help="Process all available Bronze commits and exit (default)"
    )
    mode.add_argument(
        "--continuous", action="store_true", help="Keep watching Bronze for new commits"
    )
    parser.add_argument("--interval", default="60 seconds")
    parser.add_argument(
        "--max-bytes-per-trigger",
        default="128m",
        help="Soft input-size limit for each Delta microbatch",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    from src.common.spark import create_spark

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = create_spark("silver-cdc", args.master)
    try:
        run_silver(
            spark,
            args.bronze,
            args.silver,
            args.rejected,
            args.audit,
            args.checkpoint,
            continuous=args.continuous,
            interval=args.interval,
            max_bytes_per_trigger=args.max_bytes_per_trigger,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
