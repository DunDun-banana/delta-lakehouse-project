"""Incremental Bronze -> Silver via Delta streaming and foreachBatch MERGE.

Run from repository root: python -m src.silver.silver_pipeline --once
"""
from __future__ import annotations
import argparse
import json
import logging
import os
from pathlib import Path
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from .silver_cleaning import validate_and_split, deduplicate
from .silver_merge import merge_silver, merge_rejected
from src.audit.time_travel import latest_commit

ROOT = Path(__file__).resolve().parents[2]
LOG = logging.getLogger("silver")


def make_spark(master: str | None = None) -> SparkSession:
    # 1. Định vị và tạo sẵn thư mục tạm 'data/tmp' trên ổ D (theo đường dẫn dự án ROOT)
    spark_tmp_dir = str(ROOT / "data" / "tmp")
    Path(spark_tmp_dir).mkdir(parents=True, exist_ok=True)

    builder = (SparkSession.builder.appName("Taxi-Silver-CDC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SILVER_SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.getenv("SILVER_DRIVER_MEMORY", "4g"))  # Cấp 4GB RAM Heap cho Driver
        .config("spark.sql.ansi.enabled", "false")  # Invalid demo timestamps -> null (Spark 3.4)
        # 2. Ép Spark ghi toàn bộ file tạm Shuffle/Scratch sang đĩa D thay vì ổ C
        .config("spark.local.dir", spark_tmp_dir)
        )

    
    if master:
        builder = builder.master(master)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def process_batch(spark: SparkSession, df, batch_id: int, silver: str, rejected_path: str, audit_path: str):
    # Spark may replay this microbatch after a crash; both data writes are MERGEs.
    if df.isEmpty():
        return
    df = df.persist()
    valid = rejected = winners = None
    try:
        valid, rejected = validate_and_split(df)

        # Cache only raw microbatch and deduplicated winners to reduce local disk spill.
        winners = deduplicate(valid).persist()

        incoming = df.count()
        valid_count = valid.count()
        rejected_count = rejected.count()
        winner_count = winners.count()
        version_before = latest_commit(spark, silver)
        if winner_count:
            merge_silver(spark, winners, silver)
        version_after = latest_commit(spark, silver)
        if rejected_count:
            merge_rejected(spark, rejected, rejected_path)
        
        # This audit is a *microbatch summary*, not an atomic transaction
        # across the Silver and quarantine tables.
        
        # audit = spark.createDataFrame([(
        #     int(batch_id), int(incoming), int(valid_count), int(rejected_count),
        #     int(winner_count))],
        #     "batch_id long, incoming long, valid long, rejected long, winners long")
        # audit = (audit.withColumn("processed_at", F.current_timestamp())
        #     .withColumn("silver_version_before", F.lit(version_before["version"] if version_before else None).cast("long"))
        #     .withColumn("silver_version_after", F.lit(version_after["version"] if version_after else None).cast("long"))
        #     .withColumn("silver_operation", F.lit(version_after["operation"] if version_after else None))
        #     .withColumn("silver_operation_metrics", F.lit(json.dumps(version_after["operationMetrics"] if version_after else {})))
        #     .withColumn("silver_commit_timestamp", F.lit(version_after["timestamp"] if version_after else None)))
        

        # Create audit row using Spark SQL expressions instead of PythonRDD.
        # Avoid launching a Python worker solely for audit DataFrame creation.

        audit = spark.range(1).select(
            F.lit(int(batch_id)).cast("long").alias("batch_id"),
            F.lit(int(incoming)).cast("long").alias("incoming"),
            F.lit(int(valid_count)).cast("long").alias("valid"),
            F.lit(int(rejected_count)).cast("long").alias("rejected"),
            F.lit(int(winner_count)).cast("long").alias("winners"),
            F.current_timestamp().alias("processed_at"),
            F.lit(
                version_before["version"]
                if version_before else None
            ).cast("long").alias("silver_version_before"),
            F.lit(
                version_after["version"]
                if version_after else None
            ).cast("long").alias("silver_version_after"),
            F.lit(
                version_after["operation"]
                if version_after else None
            ).cast("string").alias("silver_operation"),
            F.lit(
                json.dumps(
                    version_after["operationMetrics"]
                    if version_after else {}
                )
            ).alias("silver_operation_metrics"),
            F.lit(
                version_after["timestamp"]
                if version_after else None
            ).cast("string").alias("silver_commit_timestamp"),
        )

        if not DeltaTable.isDeltaTable(spark, audit_path):
            audit.write.format("delta").mode("errorifexists").save(audit_path)
        else:
            (DeltaTable.forPath(spark, audit_path).alias("t")
             .merge(audit.alias("s"), "t.batch_id = s.batch_id")
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        LOG.info(json.dumps({"event":"silver_batch", "batch_id":batch_id,
            "silver_version_before":version_before["version"] if version_before else None,
            "silver_version_after":version_after["version"] if version_after else None,
            "silver_operation":version_after["operation"] if version_after else None,
            "incoming":incoming,"valid":valid_count,"rejected":rejected_count,
            "winners":winner_count}))
    finally:
        if winners is not None: winners.unpersist()
        if rejected is not None: rejected.unpersist()
        if valid is not None: valid.unpersist()
        df.unpersist()


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bronze", default=str(ROOT / "data/bronze/taxi_trips"))
    p.add_argument("--silver", default=str(ROOT / "data/silver/taxi_trips"))
    p.add_argument("--rejected", default=str(ROOT / "data/silver/rejected_records"))
    p.add_argument("--audit", default=str(ROOT / "data/silver/batch_audit"))
    p.add_argument("--checkpoint", default=str(ROOT / "data/checkpoints/silver_taxi"))
    p.add_argument("--master", default="local[2]")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Process all available Bronze commits and exit")
    mode.add_argument("--continuous", action="store_true", help="Keep watching Bronze for new commits")
    p.add_argument("--interval", default="60 seconds")
    p.add_argument("--max-bytes-per-trigger", default="128m",
                   help="Soft input-size limit for each Delta microbatch")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = make_spark(args.master)
    try:
        if not DeltaTable.isDeltaTable(spark, args.bronze):
            raise FileNotFoundError("Bronze Delta table missing: " + args.bronze)
        for p in (args.silver, args.rejected, args.audit, args.checkpoint):
            Path(p).parent.mkdir(parents=True, exist_ok=True)
        # Delta microbatch input option: NOT spark.sql.streaming.maxBytesPerTrigger.
        stream = (spark.readStream.format("delta")
                  .option("maxBytesPerTrigger", args.max_bytes_per_trigger)
                  .load(args.bronze))
        writer = (stream.writeStream.queryName("silver_taxi_cdc")
            .option("checkpointLocation", args.checkpoint)
            .foreachBatch(lambda batch, bid: process_batch(
                spark, batch, bid, args.silver, args.rejected, args.audit)))
        if args.continuous:
            writer = writer.trigger(processingTime=args.interval)
        else:
            writer = writer.trigger(availableNow=True)
        query = writer.start()
        query.awaitTermination()
        if query.exception():
            raise RuntimeError(str(query.exception()))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
