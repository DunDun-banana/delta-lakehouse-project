"""Append controlled CDC demo events into Bronze (Spark 3.4 / Delta 2.4).

Run after initial Silver finishes:
  python -m scripts.demo_silver_cdc --mode update
  python -m src.silver.silver_pipeline --once
  python -m scripts.demo_silver_cdc --mode insert
  python -m src.silver.silver_pipeline --once
  python -m scripts.demo_silver_cdc --mode schema
  python -m src.silver.silver_pipeline --once

This is a *synthetic ingestion-time CDC* demo, not a real TLC change feed.
"""
import argparse
import hashlib
from datetime import datetime, timezone
from pyspark.sql import functions as F
from src.silver.silver_pipeline import make_spark, ROOT

trip_id_demo = '93184dd0b5a98cc1efe037f60a0ab8f5f90c178bbb12809ac61b991e106c9c05'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("update", "insert", "schema"))
    parser.add_argument("--bronze", default=str(ROOT / "data/bronze/taxi_trips"))
    parser.add_argument("--silver", default=str(ROOT / "data/silver/taxi_trips"))
    parser.add_argument("--trip-id", default=trip_id_demo,help="Choose an existing Silver trip explicitly")
    args = parser.parse_args()
    spark = make_spark("local[2]")
    try:
        # update mode: existing trip_id, fare_amount +2, tip_amount +1
        # Choose a trip that is ALREADY in Silver so UPDATE is actually demonstrated.
        silver = spark.read.format("delta").load(args.silver)
        trip_id = args.trip_id
        if not trip_id:
            first = silver.select("trip_id").limit(1).collect()
            if not first:
                raise RuntimeError("Silver empty: fix cleaning and finish initial Silver first")
            trip_id = first[0][0]
        source = spark.read.format("delta").load(args.bronze)
        latest = (source.filter(F.col("trip_id") == trip_id)
                  .orderBy(F.col("ingested_at").desc(), F.col("raw_record_hash").desc())
                  .limit(1))
        if latest.isEmpty():
            raise RuntimeError(f"Cannot find {trip_id} in Bronze")
        stamp = datetime.now(timezone.utc).strftime("demo_%Y%m%dT%H%M%S%f")
        row = latest

        # insert mode: new trip_id, pickup/dropoff +1 day, new hash
        new_trip_id = None

        if args.mode == "insert":
            # Generate a genuinely new trip_id.
            new_trip_id = hashlib.sha256(
                f"new_demo_trip:{stamp}:{trip_id}".encode("utf-8")
            ).hexdigest()

            row = (
                row
                .withColumn("trip_id", F.lit(new_trip_id))
                .withColumn(
                    "tpep_pickup_datetime",
                    F.date_format(
                        F.to_timestamp("tpep_pickup_datetime")
                        + F.expr("INTERVAL 1 DAY"),
                        "yyyy-MM-dd HH:mm:ss"
                    )
                )
                .withColumn(
                    "tpep_dropoff_datetime",
                    F.date_format(
                        F.to_timestamp("tpep_dropoff_datetime")
                        + F.expr("INTERVAL 1 DAY"),
                        "yyyy-MM-dd HH:mm:ss"
                    )
                )
            )

        else:
            row = (
                row
                .withColumn(
                    "fare_amount",
                    F.col("fare_amount") + F.lit(2.0)
                )
                .withColumn(
                    "tip_amount",
                    F.coalesce(
                        F.col("tip_amount"),
                        F.lit(0.0)
                    ) + F.lit(1.0)
                )
            )

        # schema mode: add a new column surcharge_fee, which will be null for existing rows
        if args.mode == "schema":
            row = row.withColumn("surcharge_fee", F.lit(1.5).cast("double"))
        row = (row.withColumn("ingested_at", F.current_timestamp())
               .withColumn("ingest_batch_id", F.lit(stamp))
               .withColumn("record_source", F.lit("cdc_demo")))
        if "source_file" in row.columns:
            row = row.withColumn("source_file", F.lit("demo://cdc"))
        if "source_path" in row.columns:
            row = row.withColumn("source_path", F.lit("demo://cdc"))
        payload = [c for c in row.columns if c not in
                   {"raw_record_hash", "ingested_at", "ingest_batch_id", "source_file", "source_path"}]
        row = row.withColumn("raw_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload])), 256))
        # One write action; no preceding show() on this non-deterministic timestamp DF.
        row.write.format("delta").mode("append").option("mergeSchema", "true").save(args.bronze)
        print(
            f"Appended mode={args.mode}, "
            f"ingest_batch_id={stamp}, "
            f"source_trip_id={trip_id}, "
            f"new_trip_id={new_trip_id}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
