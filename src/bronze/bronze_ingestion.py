"""Task 1 - append-only Bronze ingestion for NYC TLC Parquet data.

The official monthly Parquet files remain immutable under ``data/source``.
This module reads each input as a separate batch and appends it to one Delta
table.  The small ``data/landing/dirty_test.parquet`` fixture is appended as
another batch so Bronze preserves malformed values, missing fields, and
duplicates for later Silver validation.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "data" / "source"
DIRTY_INPUT = PROJECT_ROOT / "data" / "landing" / "dirty_test.parquet"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "taxi_trips"


def create_spark() -> SparkSession:
    """Create a local Spark session configured for Delta Lake."""

    builder = (
        SparkSession.builder
        .appName("Taxi-Bronze-Ingestion")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def create_trip_id(df: DataFrame) -> DataFrame:
    """Create a deterministic ID only for source rows that do not have one."""

    pickup = F.date_format(
        F.to_timestamp(F.col("tpep_pickup_datetime")),
        "yyyy-MM-dd HH:mm:ss",
    )
    dropoff = F.date_format(
        F.to_timestamp(F.col("tpep_dropoff_datetime")),
        "yyyy-MM-dd HH:mm:ss",
    )
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


def normalize_input(df: DataFrame) -> DataFrame:
    """Align source and dirty-fixture schemas without cleaning the data."""

    if "trip_id" not in df.columns:
        df = create_trip_id(df)

    if "record_source" not in df.columns:
        df = df.withColumn("record_source", F.lit("official_tlc"))

    # Parquet preserves the source schema exactly, so some official files use
    # integer location IDs while the generated fixture uses long IDs.  Delta
    # cannot merge those as two different column types.  These casts only
    # establish one Bronze schema; they do not remove or validate any values.
    long_columns = [
        "VendorID",
        "passenger_count",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
    ]
    double_columns = [
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
        "cbd_congestion_fee",
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
    ]
    for column in long_columns:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("long"))
    for column in double_columns:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("double"))

    # Keep dates as strings so malformed dates from the dirty fixture survive
    # Bronze ingestion and can be handled explicitly in Silver.
    for column in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("string"))

    return df


def add_bronze_metadata(df: DataFrame, input_path: Path, batch_id: str) -> DataFrame:
    """Add lineage and raw-record metadata while leaving business values intact."""

    business_columns = df.columns
    return (
        df.withColumn("source_file", F.input_file_name())
        .withColumn("source_path", F.lit(str(input_path.resolve())))
        .withColumn("ingest_batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "raw_record_hash",
            F.sha2(F.to_json(F.struct(*[F.col(column) for column in business_columns])), 256),
        )
    )


def ingest_batch(
    spark: SparkSession,
    input_path: Path,
    output_path: Path,
    batch_id: str,
) -> int:
    """Read one Parquet batch and append it to the Bronze Delta table."""

    input_path = input_path.resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input Parquet path does not exist: {input_path}")

    raw_df = spark.read.parquet(str(input_path))
    bronze_df = add_bronze_metadata(normalize_input(raw_df), input_path, batch_id)

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(output_path.resolve()))
    )

    return bronze_df.count()


def default_inputs() -> list[Path]:
    """Return all official monthly files followed by the dirty test batch."""

    monthly_files = sorted(SOURCE_DIR.glob("yellow_tripdata_*.parquet"))
    if not monthly_files:
        raise FileNotFoundError(f"No monthly Parquet files found under {SOURCE_DIR}")
    if not DIRTY_INPUT.exists():
        raise FileNotFoundError(
            f"Dirty fixture not found: {DIRTY_INPUT}. Run prepare_dirty_parquet.py first."
        )
    return [*monthly_files, DIRTY_INPUT]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        type=Path,
        help="One Parquet file/dataset. Repeat --input for multiple append batches.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Batch ID for a single input; otherwise each input stem is used.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    inputs: Iterable[Path] = args.inputs or default_inputs()
    inputs = list(inputs)
    if not inputs:
        raise ValueError("At least one Parquet input is required")
    if args.batch_id and len(inputs) != 1:
        raise ValueError("--batch-id can only be used with one --input")

    output_path = args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    spark = create_spark()
    try:
        total_rows = 0
        for input_path in inputs:
            batch_id = args.batch_id or input_path.stem
            written_rows = ingest_batch(spark, input_path, output_path, batch_id)
            total_rows += written_rows
            print(f"Appended {written_rows:,} rows from {input_path} as batch {batch_id}")

        bronze_total = spark.read.format("delta").load(str(output_path)).count()
        print(f"Rows appended in this run: {total_rows:,}")
        print(f"Bronze total rows: {bronze_total:,}")
        print(f"Bronze path: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
