"""Show the top Gold groups by driver earnings and zone 132 (JFK) by hour (read-only).

    python -m scripts.peek_gold
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.common.config import GOLD_PATH
from src.common.spark import create_spark


def display_columns() -> list[str | Column]:
    """Rounded Gold columns (built lazily: Column objects need a SparkContext)."""

    return [
        "PULocationID",
        "pickup_hour",
        "trip_count",
        F.round("avg_fare", 2).alias("avg_fare"),
        F.round("tip_pct", 1).alias("tip_pct"),
        F.round("avg_driver_earnings", 2).alias("avg_earn"),
        F.round("total_driver_earnings", 0).alias("total_earn"),
    ]


def main() -> None:
    spark = create_spark("peek-gold")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        gold = spark.read.format("delta").load(str(GOLD_PATH))
        columns = display_columns()
        print("Top 10 (zone, hour) groups by total driver earnings:")
        gold.orderBy(F.desc("total_driver_earnings")).select(*columns).show(10)
        print("Zone 132 (JFK airport) by hour:")
        gold.filter("PULocationID = 132").orderBy("pickup_hour").select(*columns).show(24)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
