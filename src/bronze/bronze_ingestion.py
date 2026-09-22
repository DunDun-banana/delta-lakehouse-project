import argparse
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    sha2,
    struct,
    to_json,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw_json" / "batch_001.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "taxi_trips"


RAW_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
])


def create_spark():
    builder = (
        SparkSession.builder
        .appName("Taxi-Bronze-Ingestion")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest_batch(spark, input_path, output_path, batch_id):
    raw_df = (
        spark.read
        .schema(RAW_SCHEMA)
        .json(str(input_path))
    )

    data_columns = raw_df.columns

    bronze_df = (
        raw_df
        .withColumn("source_file", input_file_name())
        .withColumn("ingest_batch_id", lit(batch_id))
        .withColumn("ingested_at", current_timestamp())
        .withColumn(
            "raw_record_hash",
            sha2(
                to_json(
                    struct(*[col(c) for c in data_columns])
                ),
                256
            )
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(output_path))
    )

    return bronze_df.count()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT
    )

    parser.add_argument(
        "--batch-id",
        default=None
    )

    args = parser.parse_args()
    batch_id = args.batch_id or args.input.stem

    spark = create_spark()

    try:
        written_rows = ingest_batch(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            batch_id=batch_id,
        )

        total_rows = (
            spark.read
            .format("delta")
            .load(str(args.output))
            .count()
        )

        print(f"Appended rows: {written_rows:,}")
        print(f"Bronze total rows: {total_rows:,}")
        print(f"Bronze path: {args.output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    
