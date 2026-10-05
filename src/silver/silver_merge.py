"""Idempotent Delta MERGEs for the Silver table and the rejected-records table."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.delta_utils import schema_auto_merge
from src.silver.schema_evolution import check_evolution

LOG = logging.getLogger("silver")

# A source row replaces the target only if it was ingested later. Equal
# timestamps break ties on batch ID then raw hash, so replays are deterministic.
NEWER = (
    "s.ingested_at > t.ingested_at OR "
    "(s.ingested_at = t.ingested_at AND "
    "(s.ingest_batch_id > t.ingest_batch_id OR "
    "(s.ingest_batch_id = t.ingest_batch_id AND "
    "s.raw_record_hash > t.raw_record_hash)))"
)
# ...and only if the business content changed. Comparing business_hash (not
# raw_record_hash) stops a re-ingested copy with identical amounts, such as
# the dirty fixture, from overwriting the official row.
UPDATE_CONDITION = f"({NEWER}) AND NOT (s.business_hash <=> t.business_hash)"


def merge_silver(spark: SparkSession, updates: DataFrame, silver_path: str) -> None:
    """Upsert deduplicated trips by ``trip_id`` (UPDATE newer+changed, INSERT new)."""

    if not DeltaTable.isDeltaTable(spark, silver_path):
        (
            updates.write.format("delta")
            .mode("errorifexists")
            .option("mergeSchema", "true")
            .save(silver_path)
        )
        return

    target = DeltaTable.forPath(spark, silver_path)
    new_columns = check_evolution(updates, target.toDF())
    if new_columns:
        LOG.info("Silver schema evolution adds columns: %s", new_columns)
    with schema_auto_merge(spark):
        (
            target.alias("t")
            .merge(updates.alias("s"), "t.trip_id = s.trip_id")
            .whenMatchedUpdateAll(condition=UPDATE_CONDITION)
            .whenNotMatchedInsertAll()
            .execute()
        )


def merge_rejected(spark: SparkSession, rejected: DataFrame, path: str) -> None:
    """Insert rejected rows once per (batch, raw hash) so retries add no duplicates."""

    rejected = rejected.withColumn("rejected_at", F.current_timestamp()).dropDuplicates(
        ["ingest_batch_id", "raw_record_hash"]
    )
    if not DeltaTable.isDeltaTable(spark, path):
        rejected.write.format("delta").mode("errorifexists").option("mergeSchema", "true").save(path)
        return
    with schema_auto_merge(spark):
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(
                rejected.alias("s"),
                "t.ingest_batch_id = s.ingest_batch_id AND t.raw_record_hash = s.raw_record_hash",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
