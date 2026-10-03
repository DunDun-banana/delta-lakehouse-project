"""Profile zero-duration trips by VendorID (read-only).

Shows that zero duration comes from one vendor that does not record the
dropoff time while fares and distances look normal, which is why Gold keeps
these trips.

    python -m scripts.peek_zero_duration
"""

from __future__ import annotations

from pyspark.sql import functions as F

from src.common.config import SILVER_PATH
from src.common.spark import create_spark


def main() -> None:
    spark = create_spark("peek-zero-duration")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        silver = spark.read.format("delta").load(str(SILVER_PATH)).withColumn(
            "is_zero", F.col("tpep_dropoff_datetime") == F.col("tpep_pickup_datetime")
        )
        (
            silver.groupBy("is_zero", "VendorID")
            .agg(
                F.count("*").alias("trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            )
            .orderBy("is_zero", "VendorID")
            .show(50)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
