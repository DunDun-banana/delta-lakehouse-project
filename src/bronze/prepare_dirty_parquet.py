"""Create a small Parquet fixture for the Bronze ingestion demo.

The official TLC files under ``data/source`` are kept immutable.  This script
reads a small sample from one monthly file and creates one generated landing
dataset:

* ``dirty_test.parquet`` contains malformed dates, missing locations,
  missing coordinates, invalid fares, and duplicate rows.

Spark writes a Parquet dataset as a directory (despite the ``.parquet``
suffix), which is the normal format consumed by PySpark.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "source" / "yellow_tripdata_2025-01.parquet"
DEFAULT_DIRTY_OUTPUT = PROJECT_ROOT / "data" / "landing" / "dirty_test.parquet"


def build_spark() -> SparkSession:
    """Create the local Spark session used by this fixture generator."""

    return (
        SparkSession.builder
        .appName("PrepareTaxiParquetFixtures")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def add_trip_id(df: DataFrame) -> DataFrame:
    """Add a deterministic ID to the dirty fixture.

    Bronze ingestion uses the same expression for official source rows that
    do not already contain a trip ID.
    """

    pickup = F.date_format(F.to_timestamp("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
    dropoff = F.date_format(F.to_timestamp("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
    identity = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit(""))
            for column in ["VendorID", "PULocationID", "DOLocationID", "trip_distance"]
        ],
        F.coalesce(pickup, F.lit("")),
        F.coalesce(dropoff, F.lit("")),
    )
    return df.withColumn("trip_id", F.sha2(identity, 256))


def add_coordinates(df: DataFrame) -> DataFrame:
    """Add synthetic coordinates for testing null-coordinate handling.

    TLC trip records contain location IDs rather than GPS coordinates.  These
    values are deliberately synthetic and are only used to exercise the
    Bronze/Silver data-quality logic.
    """

    return (
        df.withColumn(
            "pickup_latitude",
            F.when(F.col("PULocationID").isNotNull(), F.lit(40.750000)).cast("double"),
        )
        .withColumn(
            "pickup_longitude",
            F.when(F.col("PULocationID").isNotNull(), F.lit(-73.980000)).cast("double"),
        )
        .withColumn(
            "dropoff_latitude",
            F.when(F.col("DOLocationID").isNotNull(), F.lit(40.730000)).cast("double"),
        )
        .withColumn(
            "dropoff_longitude",
            F.when(F.col("DOLocationID").isNotNull(), F.lit(-73.990000)).cast("double"),
        )
    )


def load_sample(spark: SparkSession, source: Path, sample_size: int) -> DataFrame:
    """Read and prepare a bounded sample from an official monthly Parquet file."""

    if sample_size < 100:
        raise ValueError("sample_size must be at least 100 rows")

    source_path = source.resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source Parquet file does not exist: {source_path}")

    source_df = spark.read.parquet(str(source_path))
    required_columns = {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "tip_amount",
        "total_amount",
    }
    missing = sorted(required_columns.difference(source_df.columns))
    if missing:
        raise ValueError(f"Source Parquet is missing required columns: {', '.join(missing)}")

    return (
        source_df.limit(sample_size)
        .transform(add_trip_id)
        .transform(add_coordinates)
        .withColumn("record_source", F.lit("generated_fixture"))
        .withColumn("_fixture_row_id", F.monotonically_increasing_id())
        # Strings allow the fixture to carry deliberately malformed dates.
        .withColumn(
            "tpep_pickup_datetime",
            F.date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.date_format("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"),
        )
    )


def build_dirty_fixture(base: DataFrame) -> DataFrame:
    """Inject the quality issues required by the Bronze task."""

    row_id = F.col("_fixture_row_id")
    dirty = (
        base.withColumn(
            "tpep_pickup_datetime",
            F.when(F.pmod(row_id, 13) == 0, F.lit("not-a-date"))
            .otherwise(F.col("tpep_pickup_datetime")),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.when(F.pmod(row_id, 29) == 0, F.lit("2025-99-99 25:61:00"))
            .otherwise(F.col("tpep_dropoff_datetime")),
        )
        .withColumn(
            "PULocationID",
            F.when(F.pmod(row_id, 17) == 0, F.lit(None).cast("long"))
            .otherwise(F.col("PULocationID")),
        )
        .withColumn(
            "DOLocationID",
            F.when(F.pmod(row_id, 19) == 0, F.lit(None).cast("long"))
            .otherwise(F.col("DOLocationID")),
        )
        .withColumn(
            "pickup_latitude",
            F.when(F.pmod(row_id, 7) == 0, F.lit(None).cast("double"))
            .otherwise(F.col("pickup_latitude")),
        )
        .withColumn(
            "pickup_longitude",
            F.when(F.pmod(row_id, 7) == 0, F.lit(None).cast("double"))
            .otherwise(F.col("pickup_longitude")),
        )
        .withColumn(
            "dropoff_latitude",
            F.when(F.pmod(row_id, 11) == 0, F.lit(None).cast("double"))
            .otherwise(F.col("dropoff_latitude")),
        )
        .withColumn(
            "dropoff_longitude",
            F.when(F.pmod(row_id, 11) == 0, F.lit(None).cast("double"))
            .otherwise(F.col("dropoff_longitude")),
        )
        .withColumn(
            "fare_amount",
            F.when(F.pmod(row_id, 101) == 0, F.lit(-1.0))
            .otherwise(F.col("fare_amount")),
        )
    )

    # Keep exact duplicate records so Silver can demonstrate deduplication.
    duplicate_count = max(1, base.count() // 20)
    duplicates = dirty.filter(F.pmod(row_id, 23) == 0).limit(duplicate_count)
    return dirty.unionByName(duplicates).drop("_fixture_row_id")


def write_fixture(df: DataFrame, output: Path) -> None:
    """Write a generated Parquet dataset, replacing only this generated output."""

    output_path = output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").parquet(str(output_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--dirty-output", type=Path, default=DEFAULT_DIRTY_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=5_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark()
    try:
        base = load_sample(spark, args.source, args.sample_size).cache()
        dirty = build_dirty_fixture(base).cache()

        write_fixture(dirty, args.dirty_output)

        print(f"Created {args.dirty_output.resolve()} with {dirty.count():,} rows")
        print("Dirty fixture schema:")
        dirty.printSchema()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
