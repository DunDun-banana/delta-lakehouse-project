"""Delta smoke test for Task 3 on a tiny temporary table (v0 write, v1-v3 MERGE).

Skipped by default because it starts Spark. Run with:
    $env:RUN_SPARK_TESTS = "1"; .venv311\\Scripts\\python.exe -m pytest -q tests\\test_time_travel_spark.py
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(os.getenv("RUN_SPARK_TESTS") == "1", "set RUN_SPARK_TESTS=1")
class RunAuditSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from src.silver.silver_pipeline import make_spark

        os.environ.setdefault("SILVER_DRIVER_MEMORY", "1g")
        os.environ.setdefault("SILVER_SHUFFLE_PARTITIONS", "2")
        cls.spark = make_spark("local[1]")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        cls.path = str(Path(cls.temp_dir.name) / "silver")
        cls._build_table()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.temp_dir.cleanup()

    @classmethod
    def _row(cls, trip_id: str, fare: float, tip: float, batch: str, **extra: float):
        # Pure JVM expressions: no Python worker is needed to build test rows.
        from pyspark.sql import functions as F

        columns = [
            F.lit(trip_id).alias("trip_id"),
            F.lit(fare).alias("fare_amount"),
            F.lit(tip).alias("tip_amount"),
            F.lit(batch).alias("ingest_batch_id"),
            F.lit("test").alias("record_source"),
        ]
        columns += [F.lit(value).cast("double").alias(name) for name, value in extra.items()]
        return cls.spark.range(1).select(*columns)

    @classmethod
    def _merge(cls, source) -> None:
        from delta.tables import DeltaTable

        (
            DeltaTable.forPath(cls.spark, cls.path).alias("t")
            .merge(source.alias("s"), "t.trip_id = s.trip_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    @classmethod
    def _build_table(cls) -> None:
        initial = (
            cls._row("trip-0", 10.0, 1.0, "b0")
            .unionByName(cls._row("trip-1", 11.0, 1.0, "b0"))
            .unionByName(cls._row("trip-2", 12.0, 1.0, "b0"))
        )
        initial.write.format("delta").save(cls.path)  # v0 WRITE
        cls._merge(cls._row("trip-0", 14.0, 3.0, "b1"))  # v1 UPDATE
        cls._merge(cls._row("trip-new", 20.0, 2.0, "b2"))  # v2 INSERT
        cls._merge(cls._row("trip-1", 13.0, 2.0, "b3", surcharge_fee=1.5))  # v3 schema

    def test_run_audit_passes_and_is_read_only(self) -> None:
        from src.audit.time_travel import latest_commit, run_audit

        before = latest_commit(self.spark, self.path)["version"]
        evidence = run_audit(self.spark, self.path, history_limit=10)

        self.assertEqual(evidence["verification"], "PASS", evidence["checks"])
        self.assertEqual(evidence["method"], "file_scoped")
        self.assertEqual(evidence["selected_versions"]["update_after_version"], 1)
        self.assertEqual(evidence["selected_versions"]["insert_after_version"], 2)
        self.assertEqual(evidence["update_record"]["trip_id"], "trip-0")
        self.assertEqual(evidence["update_record"]["before_fare_amount"], 10.0)
        self.assertEqual(evidence["update_record"]["after_fare_amount"], 14.0)
        self.assertEqual(evidence["insert_record"]["trip_id"], "trip-new")
        self.assertEqual(evidence["schema_introduction"]["version"], 3)
        self.assertEqual(evidence["schema_value_record"]["surcharge_fee"], 1.5)
        self.assertEqual(evidence["protocol"]["log_version"], 0)
        self.assertEqual(evidence["snapshot_counts"], {0: 3, 1: 3, 2: 4})
        self.assertEqual(latest_commit(self.spark, self.path)["version"], before)


if __name__ == "__main__":
    unittest.main()
