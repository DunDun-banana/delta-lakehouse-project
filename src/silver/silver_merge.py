from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from .schema_evolution import check_evolution


def merge_silver(spark: SparkSession, updates: DataFrame, silver_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        (updates.write.format("delta").mode("errorifexists")
         .option("mergeSchema", "true").save(silver_path))
        return
    target = DeltaTable.forPath(spark, silver_path)
    check_evolution(updates, target.toDF())
    
    # Only an ingestion newer than the current row may replace it; same-timestamp
    # ties are deterministic, and replays do not replace identical content.
    newer = ("s.ingested_at > t.ingested_at OR "
             "(s.ingested_at = t.ingested_at AND "
             "(s.ingest_batch_id > t.ingest_batch_id OR "
             "(s.ingest_batch_id = t.ingest_batch_id AND "
             "s.raw_record_hash > t.raw_record_hash)))")
    
    # delta-spark 2.4.0 does not expose DeltaMergeBuilder.withSchemaEvolution()
    # Delta 2.4 enables additive MERGE schema evolution through this session config.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    (target.alias("t").merge(updates.alias("s"), "t.trip_id = s.trip_id")
     .whenMatchedUpdateAll(condition=newer)
     .whenNotMatchedInsertAll()
     .execute())


def merge_rejected(spark: SparkSession, rejected: DataFrame, path: str) -> None:
    # Avoid duplicating rejected rows after a failed/retried microbatch.
    rejected = (rejected.withColumn("rejected_at", F.current_timestamp())
                .dropDuplicates(["ingest_batch_id", "raw_record_hash"]))
    if not DeltaTable.isDeltaTable(spark, path):
        (rejected.write.format("delta").mode("errorifexists")
         .option("mergeSchema", "true").save(path))
        return
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(rejected.alias("s"),
            "t.ingest_batch_id = s.ingest_batch_id AND t.raw_record_hash = s.raw_record_hash")
     .whenNotMatchedInsertAll().execute())
