"""Profile Silver distributions to choose the Gold outlier thresholds (read-only).

One pass over Silver computes percentiles, maxima and how many trips each
candidate threshold would remove.

    python -m scripts.profile_outliers
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.common.config import SILVER_PATH
from src.common.spark import create_spark

COLUMNS = ["fare_amount", "trip_distance", "duration_min", "tip_amount"]
PERCENTILES = [0.5, 0.99, 0.999, 0.9999]


def candidates() -> dict[str, Column]:
    """Each condition is true for a trip that the threshold would remove.

    Built lazily because Column objects need an active SparkContext.
    """

    return {
        "fare_gt_500": F.col("fare_amount") > 500,
        "fare_gt_1000": F.col("fare_amount") > 1000,
        "distance_gt_100": F.col("trip_distance") > 100,
        "distance_gt_200": F.col("trip_distance") > 200,
        "duration_le_0": F.col("duration_min") <= 0,
        "duration_gt_6h": F.col("duration_min") > 360,
        "duration_gt_24h": F.col("duration_min") > 1440,
        "pickup_not_2025": F.year("tpep_pickup_datetime") != 2025,
    }


def main() -> None:
    spark = create_spark("profile-outliers")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        silver = spark.read.format("delta").load(str(SILVER_PATH)).withColumn(
            "duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
            / 60,
        )
        thresholds = candidates()
        aggregations = [F.count("*").alias("total")]
        for column in COLUMNS:
            aggregations.append(
                F.percentile_approx(column, PERCENTILES, 100000).alias(f"{column}__pct")
            )
            aggregations.append(F.max(column).alias(f"{column}__max"))
        for name, condition in thresholds.items():
            aggregations.append(F.sum(condition.cast("long")).alias(name))

        result = silver.agg(*aggregations).first().asDict()
        total = result["total"]
        print(f"\nSilver trips: {total:,}\n")
        print("column              p50        p99      p99.9     p99.99        max")
        for column in COLUMNS:
            p = result[f"{column}__pct"]
            print(
                f"{column:15s} {p[0]:>9.2f} {p[1]:>10.2f} {p[2]:>10.2f} "
                f"{p[3]:>10.2f} {result[column + '__max']:>10.2f}"
            )
        print("\nthreshold              trips removed      share")
        for name in thresholds:
            removed = result[name]
            print(f"{name:22s} {removed:>18,}   {removed / total * 100:>8.4f}%")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
