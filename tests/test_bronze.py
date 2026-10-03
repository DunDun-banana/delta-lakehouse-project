"""Bronze: deterministic trip_id and schema normalization without cleaning."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.bronze.bronze_ingestion import normalize_input
from src.common.ids import add_trip_id

RAW_SCHEMA = (
    "VendorID int, PULocationID int, DOLocationID int, trip_distance double, "
    "tpep_pickup_datetime string, tpep_dropoff_datetime string, fare_amount double"
)


def raw(spark, *rows):
    return spark.createDataFrame(list(rows), RAW_SCHEMA)


def ids(df):
    return [row["trip_id"] for row in add_trip_id(df).collect()]


def test_trip_id_is_deterministic_sha256(spark):
    row = (1, 132, 236, 17.5, "2025-11-01 10:00:00", "2025-11-01 10:40:00", 70.0)
    first, second = ids(raw(spark, row, row))
    assert first == second
    assert len(first) == 64


def test_trip_id_changes_with_identity_columns(spark):
    a = (1, 132, 236, 17.5, "2025-11-01 10:00:00", "2025-11-01 10:40:00", 70.0)
    b = (1, 132, 236, 17.5, "2025-11-01 10:00:01", "2025-11-01 10:40:00", 70.0)
    first, second = ids(raw(spark, a, b))
    assert first != second


def test_trip_id_ignores_non_identity_columns(spark):
    a = (1, 132, 236, 17.5, "2025-11-01 10:00:00", "2025-11-01 10:40:00", 70.0)
    b = (1, 132, 236, 17.5, "2025-11-01 10:00:00", "2025-11-01 10:40:00", 99.0)
    first, second = ids(raw(spark, a, b))
    assert first == second


def test_null_in_different_positions_does_not_collide(spark):
    # (NULL, 1) and (1, NULL) would both be "1" without the empty-string slot.
    a = (None, 1, 5, 1.0, "2025-11-01 10:00:00", "2025-11-01 10:10:00", 10.0)
    b = (1, None, 5, 1.0, "2025-11-01 10:00:00", "2025-11-01 10:10:00", 10.0)
    first, second = ids(raw(spark, a, b))
    assert first != second


def test_normalize_keeps_dirty_values_and_widens_types(spark):
    df = raw(spark, (1, 132, 236, 2.0, "not-a-date", "2025-11-01 10:40:00", -1.0))
    out = normalize_input(df)
    row = out.first()

    assert row["tpep_pickup_datetime"] == "not-a-date"
    assert row["fare_amount"] == -1.0
    assert dict(out.dtypes)["PULocationID"] == "bigint"
    assert dict(out.dtypes)["VendorID"] == "bigint"
    assert row["record_source"] == "official_tlc"
    assert row["trip_id"] is not None


def test_normalize_keeps_existing_fixture_lineage(spark):
    df = raw(spark, (1, 1, 1, 1.0, "2025-11-01 10:00:00", "2025-11-01 10:01:00", 5.0))
    df = df.withColumn("trip_id", F.lit("fixed")).withColumn(
        "record_source", F.lit("generated_fixture")
    )
    row = normalize_input(df).first()
    assert row["trip_id"] == "fixed"
    assert row["record_source"] == "generated_fixture"
