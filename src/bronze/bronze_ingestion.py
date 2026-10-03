"""Task 1 - append-only Bronze ingestion for NYC TLC Parquet data.

Each input (one monthly TLC file, or the dirty fixture) is one batch and one
Delta append commit. Bronze keeps values as they arrive: malformed dates,
missing zones, negative fares and duplicates all survive so Silver can reject
or deduplicate them with an explicit reason.

Re-running is safe: a batch whose ``ingest_batch_id`` (the input file stem)
already exists in Bronze is skipped instead of appended twice.

Run from the project root:
    python -m src.bronze.bronze_ingestion
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import BRONZE_PATH, DIRTY_FIXTURE, SOURCE_DIR
from src.common.ids import add_trip_id

LOG = logging.getLogger("bronze")

# Monthly files disagree on integer widths (int32 vs int64) and Delta cannot
# merge two types for one column. Casting only establishes one Bronze schema;
# no value is removed or validated here.
LONG_COLUMNS = (
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
)
DOUBLE_COLUMNS = (
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
)
# Dates stay strings so malformed fixture dates reach Silver unchanged.
STRING_COLUMNS = ("tpep_pickup_datetime", "tpep_dropoff_datetime")


def normalize_input(df: DataFrame) -> DataFrame:
    """Align official and fixture schemas without cleaning any value."""

    if "trip_id" not in df.columns:
        df = add_trip_id(df)
    if "record_source" not in df.columns:
        df = df.withColumn("record_source", F.lit("official_tlc"))

    for columns, spark_type in (
        (LONG_COLUMNS, "long"),
        (DOUBLE_COLUMNS, "double"),
        (STRING_COLUMNS, "string"),
    ):
        for column in columns:
            if column in df.columns:
                df = df.withColumn(column, F.col(column).cast(spark_type))
    return df


def add_bronze_metadata(df: DataFrame, input_path: Path, batch_id: str) -> DataFrame:
    """Add lineage columns and a hash of the raw business values."""

    business_columns = df.columns
    return (
        df.withColumn("source_file", F.input_file_name())
        .withColumn("source_path", F.lit(str(input_path.resolve())))
        .withColumn("ingest_batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "raw_record_hash",
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_columns])), 256),
        )
    )


def already_ingested(spark: SparkSession, output_path: Path, batch_id: str) -> bool:
    """True if Bronze already holds at least one row of ``batch_id``."""

    if not DeltaTable.isDeltaTable(spark, str(output_path)):
        return False
    return (
        spark.read.format("delta")
        .load(str(output_path))
        .filter(F.col("ingest_batch_id") == batch_id)
        .limit(1)
        .count()
        > 0
    )


def ingest_batch(
    spark: SparkSession, input_path: Path, output_path: Path, batch_id: str
) -> int:
    """Append one Parquet input to Bronze and return the rows written."""

    input_path = input_path.resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input Parquet path does not exist: {input_path}")

    raw_df = spark.read.parquet(str(input_path))
    (
        add_bronze_metadata(normalize_input(raw_df), input_path, batch_id)
        .write.format("delta")
        .mode("append")
        # The fixture adds GPS columns that official files do not have.
        .option("mergeSchema", "true")
        .save(str(output_path))
    )
    # Read the row count from the commit metrics instead of re-scanning data.
    last_commit = DeltaTable.forPath(spark, str(output_path)).history(1).first()
    return int(last_commit["operationMetrics"]["numOutputRows"])


def default_inputs() -> list[Path]:
    """All official monthly files (sorted by name) followed by the dirty fixture."""

    monthly_files = sorted(SOURCE_DIR.glob("yellow_tripdata_*.parquet"))
    if not monthly_files:
        raise FileNotFoundError(f"No monthly Parquet files found under {SOURCE_DIR}")
    if not DIRTY_FIXTURE.exists():
        raise FileNotFoundError(
            f"Dirty fixture not found: {DIRTY_FIXTURE}. "
            "Run python -m src.bronze.prepare_dirty_parquet first."
        )
    return [*monthly_files, DIRTY_FIXTURE]


def run_bronze(
    spark: SparkSession,
    inputs: list[Path] | None = None,
    output_path: Path = BRONZE_PATH,
    batch_id: str | None = None,
) -> dict:
    """Ingest every input as one append batch and return a summary dict."""

    inputs = list(inputs) if inputs else default_inputs()
    if batch_id and len(inputs) != 1:
        raise ValueError("batch_id can only be used with exactly one input")

    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written, skipped = [], []
    for input_path in inputs:
        current_id = batch_id or input_path.stem
        if already_ingested(spark, output_path, current_id):
            LOG.info("Skip %s: already ingested into %s", current_id, output_path)
            skipped.append(current_id)
            continue
        rows = ingest_batch(spark, input_path, output_path, current_id)
        LOG.info("Appended %s rows from %s as batch %s", f"{rows:,}", input_path, current_id)
        written.append({"batch_id": current_id, "rows": rows})

    summary = {
        "event": "bronze_ingested",
        "bronze_path": str(output_path),
        "batches_written": written,
        "batches_skipped": skipped,
        "rows_appended": sum(batch["rows"] for batch in written),
        "bronze_total_rows": spark.read.format("delta").load(str(output_path)).count(),
    }
    LOG.info(json.dumps(summary))
    return summary


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        type=Path,
        help="One Parquet file/dataset. Repeat --input for several append batches.",
    )
    parser.add_argument("--output", type=Path, default=BRONZE_PATH)
    parser.add_argument(
        "--batch-id", help="Batch ID for a single --input; otherwise the input file stem."
    )
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    from src.common.spark import create_spark

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = create_spark("bronze-ingestion", args.master)
    try:
        summary = run_bronze(spark, args.inputs, args.output, args.batch_id)
        print(f"Rows appended in this run: {summary['rows_appended']:,}")
        print(f"Bronze total rows: {summary['bronze_total_rows']:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
