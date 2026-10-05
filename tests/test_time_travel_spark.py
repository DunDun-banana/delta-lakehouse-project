"""Task 3 end-to-end on a tiny Delta table: v0 WRITE, v1 UPDATE, v2 INSERT, v3 schema."""

from __future__ import annotations

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.audit.time_travel import latest_commit, run_audit
from src.common.delta_utils import schema_auto_merge


def row(spark, trip_id, fare, tip, batch, **extra):
    # Pure JVM expressions: no Python worker is needed to build test rows.
    columns = [
        F.lit(trip_id).alias("trip_id"),
        F.lit(fare).alias("fare_amount"),
        F.lit(tip).alias("tip_amount"),
        F.lit(batch).alias("ingest_batch_id"),
        F.lit("test").alias("record_source"),
    ]
    columns += [F.lit(value).cast("double").alias(name) for name, value in extra.items()]
    return spark.range(1).select(*columns)


def merge(spark, path, source):
    with schema_auto_merge(spark):
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(source.alias("s"), "t.trip_id = s.trip_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


@pytest.fixture(scope="module")
def table(spark, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("audit") / "silver")
    initial = (
        row(spark, "trip-0", 10.0, 1.0, "b0")
        .unionByName(row(spark, "trip-1", 11.0, 1.0, "b0"))
        .unionByName(row(spark, "trip-2", 12.0, 1.0, "b0"))
    )
    initial.write.format("delta").save(path)  # v0 WRITE
    merge(spark, path, row(spark, "trip-0", 14.0, 3.0, "b1"))  # v1 UPDATE
    merge(spark, path, row(spark, "trip-new", 20.0, 2.0, "b2"))  # v2 INSERT
    merge(spark, path, row(spark, "trip-1", 13.0, 2.0, "b3", surcharge_fee=1.5))  # v3 schema
    return path


def test_run_audit_passes_and_is_read_only(spark, table):
    before = latest_commit(spark, table)["version"]
    evidence = run_audit(spark, table, history_limit=10)

    assert evidence["verification"] == "PASS", evidence["checks"]
    assert evidence["method"] == "file_scoped"
    assert evidence["selected_versions"]["update_after_version"] == 1
    assert evidence["selected_versions"]["insert_after_version"] == 2
    assert evidence["update_record"]["trip_id"] == "trip-0"
    assert evidence["update_record"]["before_fare_amount"] == 10.0
    assert evidence["update_record"]["after_fare_amount"] == 14.0
    assert evidence["insert_record"]["trip_id"] == "trip-new"
    assert evidence["schema_introduction"]["version"] == 3
    assert evidence["schema_value_record"]["surcharge_fee"] == 1.5
    assert evidence["protocol"]["log_version"] == 0
    assert evidence["snapshot_counts"] == {0: 3, 1: 3, 2: 4}
    assert latest_commit(spark, table)["version"] == before


def test_version_as_of_reads_the_old_snapshot(spark, table):
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(table)
    current = spark.read.format("delta").load(table)

    assert v0.count() == 3
    assert "surcharge_fee" not in v0.columns
    assert "surcharge_fee" in current.columns
    assert v0.filter("trip_id = 'trip-0'").first()["fare_amount"] == 10.0
