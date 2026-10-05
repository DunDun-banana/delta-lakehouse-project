"""Append one controlled CDC event to Bronze for the Silver demo.

WRITES TO BRONZE. Run only after the initial Silver load has finished:

    python -m scripts.demo_silver_cdc --mode update
    python -m src.silver.silver_pipeline --once
    python -m scripts.demo_silver_cdc --mode insert
    python -m src.silver.silver_pipeline --once
    python -m scripts.demo_silver_cdc --mode schema
    python -m src.silver.silver_pipeline --once

Modes (all start from the newest Bronze row of --trip-id):
* update - same trip_id, fare_amount + 2 and tip_amount + 1,
* insert - new trip_id, pickup/dropoff shifted by one day,
* schema - like update, plus a new column surcharge_fee = 1.5.

This is synthetic ingestion-time CDC, not a real TLC change feed.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import BRONZE_PATH, SILVER_PATH
from src.common.ids import TIMESTAMP_FORMAT
from src.common.spark import create_spark

DEMO_TRIP_ID = "93184dd0b5a98cc1efe037f60a0ab8f5f90c178bbb12809ac61b991e106c9c05"
LINEAGE_COLUMNS = {"raw_record_hash", "ingested_at", "ingest_batch_id", "source_file", "source_path"}


def shift_one_day(row: DataFrame, column: str) -> DataFrame:
    shifted = F.to_timestamp(column) + F.expr("INTERVAL 1 DAY")
    return row.withColumn(column, F.date_format(shifted, TIMESTAMP_FORMAT))


def build_event(row: DataFrame, mode: str, source_trip_id: str, stamp: str) -> tuple[DataFrame, str | None]:
    """Return (event row, new trip_id or None) for one demo mode."""

    new_trip_id = None
    if mode == "insert":
        new_trip_id = hashlib.sha256(
            f"new_demo_trip:{stamp}:{source_trip_id}".encode("utf-8")
        ).hexdigest()
        row = row.withColumn("trip_id", F.lit(new_trip_id))
        row = shift_one_day(shift_one_day(row, "tpep_pickup_datetime"), "tpep_dropoff_datetime")
    else:
        row = row.withColumn("fare_amount", F.col("fare_amount") + F.lit(2.0)).withColumn(
            "tip_amount", F.coalesce(F.col("tip_amount"), F.lit(0.0)) + F.lit(1.0)
        )
    if mode == "schema":
        row = row.withColumn("surcharge_fee", F.lit(1.5).cast("double"))

    row = (
        row.withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_batch_id", F.lit(stamp))
        .withColumn("record_source", F.lit("cdc_demo"))
    )
    for column in ("source_file", "source_path"):
        if column in row.columns:
            row = row.withColumn(column, F.lit("demo://cdc"))
    payload = [c for c in row.columns if c not in LINEAGE_COLUMNS]
    row = row.withColumn(
        "raw_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload])), 256)
    )
    return row, new_trip_id


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--mode", required=True, choices=("update", "insert", "schema"))
    parser.add_argument("--bronze", default=str(BRONZE_PATH))
    parser.add_argument("--silver", default=str(SILVER_PATH))
    parser.add_argument(
        "--trip-id",
        default=DEMO_TRIP_ID,
        help="Existing Silver trip to start from; pass an empty string to use the first one",
    )
    args = parser.parse_args(argv)

    spark = create_spark("demo-silver-cdc", "local[2]")
    try:
        trip_id = args.trip_id
        if not trip_id:
            first = spark.read.format("delta").load(args.silver).select("trip_id").limit(1).collect()
            if not first:
                raise RuntimeError("Silver is empty: finish the initial Silver load first")
            trip_id = first[0][0]
        latest = (
            spark.read.format("delta")
            .load(args.bronze)
            .filter(F.col("trip_id") == trip_id)
            .orderBy(F.col("ingested_at").desc(), F.col("raw_record_hash").desc())
            .limit(1)
        )
        if latest.isEmpty():
            raise RuntimeError(f"Cannot find {trip_id} in Bronze")
        stamp = datetime.now(timezone.utc).strftime("demo_%Y%m%dT%H%M%S%f")
        event, new_trip_id = build_event(latest, args.mode, trip_id, stamp)
        # One write action; no show() first because current_timestamp() would differ.
        event.write.format("delta").mode("append").option("mergeSchema", "true").save(args.bronze)
        print(
            f"Appended mode={args.mode}, ingest_batch_id={stamp}, "
            f"source_trip_id={trip_id}, new_trip_id={new_trip_id}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
