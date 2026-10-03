"""Silver data-quality rules, business hash and deduplication.

Everything here is a lazy, distributed DataFrame transformation; no taxi row
is ever collected to the driver.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.common.ids import TIMESTAMP_FORMAT

REQUIRED = {
    "trip_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "ingested_at",
    "raw_record_hash",
    "ingest_batch_id",
}

# Columns whose change counts as a real CDC change. Lineage columns
# (record_source, ingest_batch_id, source_file, ...) and synthetic GPS are
# excluded on purpose: a re-ingested copy of the same trip with identical
# amounts must not overwrite Silver. surcharge_fee is listed so the schema
# evolution demo is detected as a change once the column exists.
CDC_COLUMNS = [
    "fare_amount", "tip_amount", "total_amount", "extra", "mta_tax", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "Airport_fee", "cbd_congestion_fee",
    "passenger_count", "payment_type", "surcharge_fee",
]

# Bounding box for the synthetic fixture coordinates (around New York City).
LAT_RANGE = (40.0, 41.5)
LON_RANGE = (-74.5, -73.0)
GPS_COLUMNS = (
    ("pickup_latitude", LAT_RANGE),
    ("pickup_longitude", LON_RANGE),
    ("dropoff_latitude", LAT_RANGE),
    ("dropoff_longitude", LON_RANGE),
)

_RAW_DATE_COLUMNS = ("raw_pickup_datetime", "raw_dropoff_datetime")


def validate_contract(df: DataFrame) -> None:
    """Fail fast if Bronze no longer provides the columns Silver relies on."""

    missing = sorted(REQUIRED - set(df.columns))
    if missing:
        raise ValueError("Bronze is missing required columns: " + ", ".join(missing))


def _invalid_gps() -> Column:
    invalid = None
    for column, (low, high) in GPS_COLUMNS:
        value = F.col(column)
        check = value.isNull() | (value < low) | (value > high)
        invalid = check if invalid is None else invalid | check
    return invalid


def quality_checks() -> list[tuple[Column, str]]:
    """Return the 9 rules as (condition that marks a row invalid, reason code).

    Expects parsed timestamp columns. The order of the list is the order of
    the reason codes in ``rejection_reasons``.
    """

    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    pu, do = F.col("PULocationID"), F.col("DOLocationID")
    fare = F.col("fare_amount")
    return [
        (F.col("trip_id").isNull() | (F.length(F.trim("trip_id")) == 0), "missing_trip_id"),
        # Unparseable strings are NULL after to_timestamp (ANSI mode is off).
        (pickup.isNull() | dropoff.isNull(), "invalid_datetime"),
        (dropoff < pickup, "dropoff_before_pickup"),
        (pu.isNull() | do.isNull() | (pu <= 0) | (do <= 0), "invalid_location"),
        # NaN is a valid double and NaN <= 0 is false in Spark, so it needs
        # its own check or it would pass the fare rule.
        (fare.isNull() | F.isnan("fare_amount") | (fare <= 0), "invalid_fare"),
        (F.col("trip_distance").isNull() | (F.col("trip_distance") < 0), "invalid_distance"),
        (F.col("total_amount").isNull() | (F.col("total_amount") <= 0), "invalid_total_amount"),
        # TLC leaves passenger_count NULL for many trips (e.g. some vendors),
        # so NULL is kept; only impossible negative counts are rejected.
        (
            F.col("passenger_count").isNotNull() & (F.col("passenger_count") < 0),
            "invalid_passenger_count",
        ),
        # Official TLC files have zone IDs but no GPS, so coordinates are only
        # checked on the generated fixture that carries synthetic GPS.
        ((F.col("record_source") == "generated_fixture") & _invalid_gps(), "invalid_coordinates"),
    ]


def add_business_hash(df: DataFrame) -> DataFrame:
    """Hash the CDC columns present in ``df``; MERGE updates only when it differs."""

    present = [c for c in CDC_COLUMNS if c in df.columns]
    return df.withColumn(
        "business_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in present])), 256)
    )


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, rejected).

    Valid rows get parsed timestamps and ``business_hash``. Rejected rows keep
    the raw date strings and every failed reason code in ``rejection_reasons``.
    """

    validate_contract(df)
    parsed = (
        df.withColumn("raw_pickup_datetime", F.col("tpep_pickup_datetime"))
        .withColumn("raw_dropoff_datetime", F.col("tpep_dropoff_datetime"))
        .withColumn(
            "tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime", TIMESTAMP_FORMAT)
        )
        .withColumn(
            "tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime", TIMESTAMP_FORMAT)
        )
    )
    reasons = F.array(*[F.when(condition, F.lit(code)) for condition, code in quality_checks()])
    checked = parsed.withColumn(
        "rejection_reasons", F.filter(reasons, lambda reason: reason.isNotNull())
    )
    valid = checked.filter(F.size("rejection_reasons") == 0).drop(
        "rejection_reasons", *_RAW_DATE_COLUMNS
    )
    rejected = checked.filter(F.size("rejection_reasons") > 0)
    return add_business_hash(valid), rejected


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one deterministic winner per ``trip_id``: the newest Bronze row.

    Bronze has no source event time, so ``ingested_at`` (processing order) is
    the recency signal; ties break on batch ID then raw hash. ``row_number``
    is used instead of ``dropDuplicates`` because dropDuplicates keeps an
    arbitrary row, which would make CDC replays non-deterministic.
    """

    window = Window.partitionBy("trip_id").orderBy(
        F.col("ingested_at").desc_nulls_last(),
        F.col("ingest_batch_id").desc_nulls_last(),
        F.col("raw_record_hash").desc_nulls_last(),
    )
    return (
        df.withColumn("_silver_rank", F.row_number().over(window))
        .filter(F.col("_silver_rank") == 1)
        .drop("_silver_rank")
    )
