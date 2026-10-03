"""Task 4 - Gold layer: business metrics by pickup zone and hour of day.

Business rules (see docs/task4_gold_optimization.md):
- Group by PULocationID and pickup hour (NYC clock time as recorded by TLC).
- avg_fare: mean of fare_amount (metered fare only, no tip/tax/surcharge).
- tip_pct: sum(tip) / sum(fare) * 100 over card trips only (payment_type = 1),
  because TLC records tips for card payments only; cash tips are always 0.
- driver earnings: fare_amount + extra + tip_amount. Taxes, surcharges and
  tolls are pass-through amounts that do not belong to the driver.
- Analytics filters drop about 0.04% implausible rows; Silver keeps them.
  duration = 0 is NOT filtered: VendorID 7 never records the dropoff time,
  while its fares and distances are normal.

Run from the project root:
    python -m src.gold.gold_aggregation
"""

from __future__ import annotations

import argparse
import json
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_PATH as _GOLD_PATH
from src.common.config import SILVER_PATH as _SILVER_PATH
from src.common.delta_utils import latest_commit

LOG = logging.getLogger("gold")

SILVER_PATH = str(_SILVER_PATH)
GOLD_PATH = str(_GOLD_PATH)

# Analytics thresholds chosen from the Silver distribution (scripts/profile_outliers.py).
MAX_FARE = 500.0                 # p99.99 = 322; max = 863,372
MAX_DISTANCE_MILES = 100.0       # p99.99 = 61; max = 397,994
MAX_DURATION_MINUTES = 6 * 60    # removes the ~24h "meter left running" cluster
ANALYSIS_YEAR = 2025
CARD_PAYMENT = 1


def apply_gold_filters(df: DataFrame) -> DataFrame:
    """Drop rows that would distort averages; Silver itself stays untouched."""

    duration_min = (
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    ) / 60
    return df.filter(
        (F.col("fare_amount") <= MAX_FARE)
        & (F.col("trip_distance") <= MAX_DISTANCE_MILES)
        & (duration_min <= MAX_DURATION_MINUTES)
        & (F.year("tpep_pickup_datetime") == ANALYSIS_YEAR)
    )


def build_gold(silver: DataFrame) -> DataFrame:
    """Aggregate filtered Silver trips into one row per (zone, hour)."""

    is_card = F.col("payment_type") == CARD_PAYMENT
    earnings = (
        F.col("fare_amount")
        + F.coalesce(F.col("extra"), F.lit(0.0))
        + F.coalesce(F.col("tip_amount"), F.lit(0.0))
    )
    card_fare = F.sum(F.when(is_card, F.col("fare_amount")))
    card_tip = F.sum(F.when(is_card, F.col("tip_amount")))

    return (
        apply_gold_filters(silver)
        .groupBy("PULocationID", F.hour("tpep_pickup_datetime").alias("pickup_hour"))
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum(is_card.cast("long")).alias("card_trip_count"),
            (card_tip / card_fare * 100).alias("tip_pct"),
            F.avg(earnings).alias("avg_driver_earnings"),
            F.sum(earnings).alias("total_driver_earnings"),
        )
    )


def write_gold(gold: DataFrame, path: str, silver_version: int) -> None:
    """Full recompute + overwrite: Gold is small, so this stays simple and idempotent."""

    (
        gold.withColumn("silver_version", F.lit(silver_version))
        .withColumn("computed_at", F.current_timestamp())
        .coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def run_gold(
    spark: SparkSession,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    silver_version: int | None = None,
) -> dict:
    """Build Gold from one pinned Silver version and return a small summary."""

    if silver_version is None:
        commit = latest_commit(spark, silver_path)
        if commit is None:
            raise FileNotFoundError(f"Silver Delta table not found: {silver_path}")
        silver_version = commit["version"]
    silver = spark.read.format("delta").option("versionAsOf", silver_version).load(silver_path)

    write_gold(build_gold(silver), gold_path, silver_version)

    gold_stats = (
        spark.read.format("delta").load(gold_path)
        .agg(F.count("*").alias("groups"), F.sum("trip_count").alias("kept"))
        .first()
    )
    total = silver.count()
    summary = {
        "event": "gold_built",
        "silver_version": silver_version,
        "silver_rows": total,
        "rows_used": int(gold_stats["kept"]),
        "rows_filtered": total - int(gold_stats["kept"]),
        "gold_groups": int(gold_stats["groups"]),
        "gold_path": gold_path,
    }
    LOG.info(json.dumps(summary))
    return summary


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--silver", default=SILVER_PATH)
    parser.add_argument("--gold", default=GOLD_PATH)
    parser.add_argument(
        "--silver-version", type=int, help="Build from a past Silver version (time travel)"
    )
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    from src.common.spark import create_spark

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = create_spark("gold-aggregation", args.master)
    try:
        run_gold(spark, args.silver, args.gold, args.silver_version)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()