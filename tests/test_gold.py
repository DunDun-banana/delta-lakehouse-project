"""Gold: metric definitions and analytics filters on a handful of trips."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.gold.gold_aggregation import build_gold

SCHEMA = (
    "PULocationID long, pickup string, dropoff string, fare_amount double, "
    "extra double, tip_amount double, trip_distance double, payment_type long"
)
CARD, CASH = 1, 2


def silver(spark, rows):
    # Timestamps are given as strings and parsed by Spark in the session time
    # zone (UTC), so the expected hours do not depend on the machine time zone.
    return (
        spark.createDataFrame(rows, SCHEMA)
        .withColumn("tpep_pickup_datetime", F.to_timestamp("pickup"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("dropoff"))
        .drop("pickup", "dropoff")
    )


@pytest.fixture(scope="module")
def gold(spark):
    rows = [
        # zone 1, 10:00 - one card and one cash trip
        (1, "2025-03-01 10:05:00", "2025-03-01 10:25:00", 10.0, 1.0, 2.0, 3.0, CARD),
        (1, "2025-03-01 10:40:00", "2025-03-01 10:55:00", 20.0, 0.0, 0.0, 4.0, CASH),
        # zone 2, 05:00 - zero duration is kept (vendor without dropoff time)
        (2, "2025-03-01 05:00:00", "2025-03-01 05:00:00", 8.0, 0.5, 1.0, 1.0, CARD),
        # outliers removed by the Gold filters
        (3, "2025-03-01 12:00:00", "2025-03-01 12:30:00", 600.0, 0.0, 0.0, 5.0, CARD),
        (3, "2025-03-01 12:00:00", "2025-03-01 12:30:00", 50.0, 0.0, 0.0, 150.0, CARD),
        (3, "2025-03-01 12:00:00", "2025-03-01 19:00:00", 50.0, 0.0, 0.0, 5.0, CARD),
        (3, "2024-12-31 12:00:00", "2024-12-31 12:30:00", 50.0, 0.0, 0.0, 5.0, CARD),
    ]
    result = build_gold(silver(spark, rows)).collect()
    return {(row["PULocationID"], row["pickup_hour"]): row for row in result}


def test_outliers_removed_and_zero_duration_kept(gold):
    assert set(gold) == {(1, 10), (2, 5)}


def test_counts_and_average_fare(gold):
    row = gold[(1, 10)]
    assert row["trip_count"] == 2
    assert row["card_trip_count"] == 1
    assert row["avg_fare"] == pytest.approx(15.0)


def test_tip_pct_uses_card_trips_only_as_sum_over_sum(gold):
    # Card trip: tip 2 / fare 10. The cash trip (tip always 0) is excluded.
    assert gold[(1, 10)]["tip_pct"] == pytest.approx(20.0)


def test_driver_earnings_are_fare_plus_extra_plus_tip(gold):
    row = gold[(1, 10)]
    # (10 + 1 + 2) and (20 + 0 + 0)
    assert row["total_driver_earnings"] == pytest.approx(33.0)
    assert row["avg_driver_earnings"] == pytest.approx(16.5)
    assert gold[(2, 5)]["total_driver_earnings"] == pytest.approx(9.5)
