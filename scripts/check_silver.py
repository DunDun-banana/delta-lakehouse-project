"""Read-only consistency check of Bronze vs Silver (scans every row, ~5 min).

Prints row reconciliation, duplicate trip_ids, rows per record_source,
rejection reasons, the last Silver commits and the table file count/size.

    python -m scripts.check_silver
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import BRONZE_PATH, REJECTED_PATH, SILVER_PATH
from src.common.spark import create_spark


def main() -> None:
    spark = create_spark("check-silver")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bronze = spark.read.format("delta").load(str(BRONZE_PATH))
        silver = spark.read.format("delta").load(str(SILVER_PATH))
        rejected = spark.read.format("delta").load(str(REJECTED_PATH))

        b, s, r = bronze.count(), silver.count(), rejected.count()
        print(f"Bronze={b:,}  Silver={s:,}  Rejected={r:,}  Deduplicated={b - s - r:,}")

        duplicates = silver.groupBy("trip_id").count().filter("count > 1").count()
        print(f"Duplicate trip_id in Silver: {duplicates} (expected 0)")

        # After the business_hash fix no generated_fixture row should win in Silver.
        silver.groupBy("record_source").count().show()

        (
            rejected.select(F.explode("rejection_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )

        table = DeltaTable.forPath(spark, str(SILVER_PATH))
        (
            table.history()
            .select(
                "version",
                "operation",
                F.col("operationMetrics.numSourceRows").alias("source"),
                F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
                F.col("operationMetrics.numTargetRowsUpdated").alias("updated"),
                F.col("operationMetrics.numTargetFilesRemoved").alias("files_removed"),
            )
            .orderBy(F.desc("version"))
            .show(5, truncate=False)
        )
        table.detail().select("numFiles", "sizeInBytes").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
