"""Silver: quality rules, business_hash and deduplication on tiny DataFrames."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.silver.silver_cleaning import deduplicate, validate_and_split

SCHEMA = (
    "trip_id string, tpep_pickup_datetime string, tpep_dropoff_datetime string, "
    "PULocationID long, DOLocationID long, fare_amount double, trip_distance double, "
    "total_amount double, tip_amount double, passenger_count long, payment_type long, "
    "record_source string, pickup_latitude double, pickup_longitude double, "
    "dropoff_latitude double, dropoff_longitude double, ingest_batch_id string, "
    "ingested_at string, raw_record_hash string"
)
FIELDS = [field.split()[0] for field in SCHEMA.split(", ")]

GOOD = {
    "trip_id": "t1",
    "tpep_pickup_datetime": "2025-11-01 10:00:00",
    "tpep_dropoff_datetime": "2025-11-01 10:30:00",
    "PULocationID": 132,
    "DOLocationID": 236,
    "fare_amount": 50.0,
    "trip_distance": 15.0,
    "total_amount": 65.0,
    "tip_amount": 10.0,
    "passenger_count": 1,
    "payment_type": 1,
    "record_source": "official_tlc",
    "pickup_latitude": None,
    "pickup_longitude": None,
    "dropoff_latitude": None,
    "dropoff_longitude": None,
    "ingest_batch_id": "b1",
    "ingested_at": "2026-01-01 00:00:00",
    "raw_record_hash": "h1",
}
FIXTURE_GPS = {
    "record_source": "generated_fixture",
    "pickup_latitude": 40.75,
    "pickup_longitude": -73.98,
    "dropoff_latitude": 40.73,
    "dropoff_longitude": -73.99,
}


def bronze(spark, *overrides):
    rows = [tuple({**GOOD, **override}[name] for name in FIELDS) for override in overrides]
    return spark.createDataFrame(rows, SCHEMA).withColumn(
        "ingested_at", F.to_timestamp("ingested_at")
    )


def reasons_by_trip(rejected):
    return {row["trip_id"]: row["rejection_reasons"] for row in rejected.collect()}


def test_valid_and_rejected_with_multiple_reasons(spark):
    df = bronze(
        spark,
        {"trip_id": "ok"},
        {"trip_id": "bad_date", "tpep_pickup_datetime": "not-a-date"},
        {"trip_id": "multi", "fare_amount": -1.0, "PULocationID": None, "total_amount": 0.0},
        {"trip_id": "backwards", "tpep_dropoff_datetime": "2025-11-01 09:00:00"},
        {"trip_id": "nan_fare", "fare_amount": float("nan")},
    )
    valid, rejected = validate_and_split(df)

    assert [row["trip_id"] for row in valid.collect()] == ["ok"]
    reasons = reasons_by_trip(rejected)
    assert reasons["bad_date"] == ["invalid_datetime"]
    assert reasons["multi"] == ["invalid_location", "invalid_fare", "invalid_total_amount"]
    assert reasons["backwards"] == ["dropoff_before_pickup"]
    assert reasons["nan_fare"] == ["invalid_fare"]


def test_rejected_rows_keep_raw_dates_and_valid_rows_get_timestamps(spark):
    df = bronze(spark, {"trip_id": "ok"}, {"trip_id": "bad", "tpep_pickup_datetime": "not-a-date"})
    valid, rejected = validate_and_split(df)

    assert dict(valid.dtypes)["tpep_pickup_datetime"] == "timestamp"
    assert "raw_pickup_datetime" not in valid.columns
    assert rejected.first()["raw_pickup_datetime"] == "not-a-date"


def test_null_passenger_count_is_kept(spark):
    valid, rejected = validate_and_split(
        bronze(spark, {"passenger_count": None}, {"trip_id": "neg", "passenger_count": -1})
    )
    assert valid.count() == 1
    assert reasons_by_trip(rejected) == {"neg": ["invalid_passenger_count"]}


def test_gps_is_checked_only_for_generated_fixture(spark):
    df = bronze(
        spark,
        {"trip_id": "official_no_gps"},
        {"trip_id": "fixture_ok", **FIXTURE_GPS},
        {"trip_id": "fixture_null_gps", **FIXTURE_GPS, "pickup_latitude": None},
        {"trip_id": "fixture_far_away", **FIXTURE_GPS, "dropoff_longitude": 2.35},
    )
    valid, rejected = validate_and_split(df)

    assert sorted(row["trip_id"] for row in valid.collect()) == ["fixture_ok", "official_no_gps"]
    assert reasons_by_trip(rejected) == {
        "fixture_null_gps": ["invalid_coordinates"],
        "fixture_far_away": ["invalid_coordinates"],
    }


def test_business_hash_ignores_lineage_and_gps(spark):
    df = bronze(
        spark,
        {"trip_id": "a"},
        {"trip_id": "b", **FIXTURE_GPS, "ingest_batch_id": "b9", "raw_record_hash": "other"},
        {"trip_id": "c", "tip_amount": 11.0},
    )
    valid, _ = validate_and_split(df)
    hashes = {row["trip_id"]: row["business_hash"] for row in valid.collect()}

    assert hashes["a"] == hashes["b"]
    assert hashes["a"] != hashes["c"]


def test_deduplicate_keeps_newest_ingestion(spark):
    df = bronze(
        spark,
        {"tip_amount": 1.0, "ingested_at": "2026-01-01 00:00:00", "ingest_batch_id": "b1"},
        {"tip_amount": 3.0, "ingested_at": "2026-01-03 00:00:00", "ingest_batch_id": "b3"},
        {"tip_amount": 2.0, "ingested_at": "2026-01-02 00:00:00", "ingest_batch_id": "b2"},
        {"trip_id": "t2"},
    )
    winners = {row["trip_id"]: row["tip_amount"] for row in deduplicate(df).collect()}
    assert winners == {"t1": 3.0, "t2": 10.0}


def test_deduplicate_breaks_timestamp_ties_by_batch_id(spark):
    df = bronze(
        spark,
        {"tip_amount": 1.0, "ingest_batch_id": "b1"},
        {"tip_amount": 2.0, "ingest_batch_id": "b2"},
    )
    assert deduplicate(df).first()["tip_amount"] == 2.0
