"""Create the dirty Parquet fixture appended to Bronze as its own batch.

The official TLC files under ``data/source`` stay immutable. This module
samples one monthly file (November 2025 by default), adds synthetic GPS
coordinates, then injects data-quality problems so Silver has something to
reject and deduplicate:

* malformed pickup / dropoff dates (stored as strings),
* NULL pickup / dropoff zones,
* NULL synthetic coordinates,
* negative fares,
* exact duplicate rows (about 5% of the sample).

Spark writes the output as a directory despite the ``.parquet`` suffix.

Run from the project root:
    python -m src.bronze.prepare_dirty_parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import DIRTY_FIXTURE, DIRTY_FIXTURE_SOURCE
from src.common.ids import TIMESTAMP_FORMAT, add_trip_id

REQUIRED_COLUMNS = {
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

# (column, modulo, value, Spark type): rows whose fixture row id is a multiple
# of ``modulo`` get ``value``. Values are plain Python data; Column literals
# are only built inside build_dirty_fixture(), never at import time.
CORRUPTIONS = (
    ("tpep_pickup_datetime", 13, "not-a-date", "string"),
    ("tpep_dropoff_datetime", 29, "2025-99-99 25:61:00", "string"),
    ("PULocationID", 17, None, "long"),
    ("DOLocationID", 19, None, "long"),
    ("pickup_latitude", 7, None, "double"),
    ("pickup_longitude", 7, None, "double"),
    ("dropoff_latitude", 11, None, "double"),
    ("dropoff_longitude", 11, None, "double"),
    ("fare_amount", 101, -1.0, "double"),
)
DUPLICATE_MODULO = 23
DUPLICATE_DIVISOR = 20  # at most 1/20 = 5% of the sample is duplicated

# Synthetic coordinates (TLC data only has zone IDs); used by the GPS rule.
SYNTHETIC_GPS = (
    ("pickup_latitude", "PULocationID", 40.75),
    ("pickup_longitude", "PULocationID", -73.98),
    ("dropoff_latitude", "DOLocationID", 40.73),
    ("dropoff_longitude", "DOLocationID", -73.99),
)


def add_coordinates(df: DataFrame) -> DataFrame:
    """Add synthetic coordinates, NULL when the related zone is NULL."""

    for column, zone_column, value in SYNTHETIC_GPS:
        df = df.withColumn(
            column, F.when(F.col(zone_column).isNotNull(), F.lit(value)).cast("double")
        )
    return df


def load_sample(spark: SparkSession, source: Path, sample_size: int) -> DataFrame:
    """Read a bounded sample of one official file and prepare it for corruption."""

    if sample_size < 100:
        raise ValueError("sample_size must be at least 100 rows")
    source_path = source.resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source Parquet file does not exist: {source_path}")

    source_df = spark.read.parquet(str(source_path))
    missing = sorted(REQUIRED_COLUMNS.difference(source_df.columns))
    if missing:
        raise ValueError(f"Source Parquet is missing required columns: {', '.join(missing)}")

    return (
        source_df.limit(sample_size)
        .transform(add_trip_id)
        .transform(add_coordinates)
        .withColumn("record_source", F.lit("generated_fixture"))
        .withColumn("_fixture_row_id", F.monotonically_increasing_id())
        # Strings allow the fixture to carry malformed dates into Bronze.
        .withColumn(
            "tpep_pickup_datetime", F.date_format("tpep_pickup_datetime", TIMESTAMP_FORMAT)
        )
        .withColumn(
            "tpep_dropoff_datetime", F.date_format("tpep_dropoff_datetime", TIMESTAMP_FORMAT)
        )
    )


def build_dirty_fixture(base: DataFrame) -> DataFrame:
    """Apply CORRUPTIONS, then append exact duplicates of some rows."""

    row_id = F.col("_fixture_row_id")
    dirty = base
    for column, modulo, value, spark_type in CORRUPTIONS:
        dirty = dirty.withColumn(
            column,
            F.when(F.pmod(row_id, modulo) == 0, F.lit(value).cast(spark_type)).otherwise(
                F.col(column)
            ),
        )

    # Keep exact duplicates (same trip_id, same values) for Silver dedup.
    duplicate_count = max(1, base.count() // DUPLICATE_DIVISOR)
    duplicates = dirty.filter(F.pmod(row_id, DUPLICATE_MODULO) == 0).limit(duplicate_count)
    return dirty.unionByName(duplicates).drop("_fixture_row_id")


def prepare_fixture(
    spark: SparkSession,
    source: Path = DIRTY_FIXTURE_SOURCE,
    output: Path = DIRTY_FIXTURE,
    sample_size: int = 5_000,
) -> int:
    """Write the dirty fixture (overwriting only this output) and return its row count."""

    base = load_sample(spark, source, sample_size).cache()
    try:
        output_path = output.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # One file is enough for ~5k rows and keeps the landing folder readable.
        build_dirty_fixture(base).coalesce(1).write.mode("overwrite").parquet(str(output_path))
    finally:
        base.unpersist()
    return spark.read.parquet(str(output_path)).count()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source", type=Path, default=DIRTY_FIXTURE_SOURCE)
    parser.add_argument("--dirty-output", type=Path, default=DIRTY_FIXTURE)
    parser.add_argument("--sample-size", type=int, default=5_000)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    from src.common.spark import create_spark

    args = parse_args(argv)
    spark = create_spark("prepare-dirty-fixture", args.master)
    try:
        rows = prepare_fixture(spark, args.source, args.dirty_output, args.sample_size)
        print(f"Created {args.dirty_output.resolve()} with {rows:,} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
