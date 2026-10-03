"""Compare one trip at two Silver versions with time travel (read-only).

    python -m scripts.peek_cdc --trip <trip_id> --before 43 --after 44
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import SILVER_PATH
from src.common.spark import create_spark


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trip", required=True)
    parser.add_argument("--before", type=int, required=True)
    parser.add_argument("--after", type=int, required=True)
    parser.add_argument("--path", default=str(SILVER_PATH))
    args = parser.parse_args(argv)

    spark = create_spark("peek-cdc")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for version in (args.before, args.after):
            df = spark.read.format("delta").option("versionAsOf", version).load(args.path)
            columns = ["fare_amount", "tip_amount", "record_source", "ingest_batch_id"]
            if "surcharge_fee" in df.columns:  # only after the schema evolution demo
                columns.append("surcharge_fee")
            print(f"--- Version {version} ---")
            df.filter(F.col("trip_id") == args.trip).select(*columns).show(truncate=False)

        (
            DeltaTable.forPath(spark, args.path)
            .history()
            .filter(F.col("version") == args.after)
            .select(
                "version",
                "operation",
                F.col("operationMetrics.numTargetRowsUpdated").alias("updated"),
                F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
                F.col("operationMetrics.numTargetFilesRemoved").alias("files_removed"),
            )
            .show()
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
