"""Distributed, lazy Silver validation; never collect individual taxi rows to driver."""
from pyspark.sql import DataFrame, functions as F, Window

REQUIRED = {"trip_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "fare_amount", "ingested_at", "raw_record_hash", "ingest_batch_id"}


def validate_contract(df: DataFrame) -> None:
    missing = sorted(REQUIRED - set(df.columns))
    if missing:
        raise ValueError("Bronze is missing required columns: " + ", ".join(missing))


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, rejected), retaining rejected raw values and reason codes."""
    validate_contract(df)
    pickup = F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
    dropoff = F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
    # Keep unmodified strings in quarantine for audit; valid rows retain clean timestamps.
    out = (df.withColumn("raw_pickup_datetime", F.col("tpep_pickup_datetime"))
             .withColumn("raw_dropoff_datetime", F.col("tpep_dropoff_datetime"))
             .withColumn("tpep_pickup_datetime", pickup)
             .withColumn("tpep_dropoff_datetime", dropoff))
    checks = [
        (F.col("trip_id").isNull() | (F.length(F.trim("trip_id")) == 0), "missing_trip_id"),
        (F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull(), "invalid_datetime"),
        (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), "dropoff_before_pickup"),
        (F.col("PULocationID").isNull() | F.col("DOLocationID").isNull() |
         (F.col("PULocationID") <= 0) | (F.col("DOLocationID") <= 0), "invalid_location"),
        (F.col("fare_amount").isNull() | F.isnan("fare_amount") | (F.col("fare_amount") <= 0), "invalid_fare"),
        (F.col("trip_distance").isNull() | (F.col("trip_distance") < 0), "invalid_distance"),
        (F.col("total_amount").isNull() | (F.col("total_amount") <= 0), "invalid_total_amount"),
        (F.col("passenger_count").isNotNull() & (F.col("passenger_count") < 0), "invalid_passenger_count"),
        
        # The real NYC TLC monthly Parquet files have location IDs but no GPS.
        # Only validate synthetic coordinates on the generated fixture.
        (
            (F.col("record_source") == "generated_fixture") & (
                F.col("pickup_latitude").isNull() |
                F.col("pickup_longitude").isNull() |
                F.col("dropoff_latitude").isNull() |
                F.col("dropoff_longitude").isNull() |
                (F.col("pickup_latitude") < 40.0) |
                (F.col("pickup_latitude") > 41.5) |
                (F.col("pickup_longitude") < -74.5) |
                (F.col("pickup_longitude") > -73.0) |
                (F.col("dropoff_latitude") < 40.0) |
                (F.col("dropoff_latitude") > 41.5) |
                (F.col("dropoff_longitude") < -74.5) |
                (F.col("dropoff_longitude") > -73.0)
            ),
            "invalid_coordinates"
        ),
    ]
    reasons = F.array(*[F.when(condition, F.lit(reason)) for condition, reason in checks])
    out = out.withColumn("rejection_reasons", F.filter(reasons, lambda x: x.isNotNull()))
    valid = (out.filter(F.size("rejection_reasons") == 0)
             .drop("rejection_reasons", "raw_pickup_datetime", "raw_dropoff_datetime"))
    rejected = out.filter(F.size("rejection_reasons") > 0)
    return valid, rejected


def deduplicate(df: DataFrame) -> DataFrame:
    """One deterministic winner per trip; newest Bronze ingestion wins.

    Bronze has no source event timestamp: ingested_at is processing order, NOT
    authoritative event-time CDC. Ties break by raw_record_hash and batch ID.
    """
    window = Window.partitionBy("trip_id").orderBy(
        F.col("ingested_at").desc_nulls_last(),
        F.col("ingest_batch_id").desc_nulls_last(),
        F.col("raw_record_hash").desc_nulls_last(),
    )
    return (df.withColumn("_silver_rank", F.row_number().over(window))
              .filter(F.col("_silver_rank") == 1).drop("_silver_rank"))
