"""Silver MERGE rules: UPDATE only when newer AND content changed; INSERT new trips."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.delta_utils import AUTO_MERGE_CONF
from src.silver.silver_merge import merge_silver


def trip(spark, record_source, tip, ingested_at, batch, trip_id="X"):
    """One Silver-ready row with every column merge_silver relies on."""

    df = spark.createDataFrame(
        [(trip_id, record_source, tip, batch, ingested_at)],
        "trip_id string, record_source string, tip_amount double, "
        "ingest_batch_id string, ingested_at string",
    )
    return (
        df.withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("raw_record_hash", F.sha2(F.concat_ws("|", "record_source", "tip_amount"), 256))
        .withColumn("business_hash", F.sha2(F.col("tip_amount").cast("string"), 256))
    )


def last_metrics(spark, path):
    return DeltaTable.forPath(spark, path).history(1).first()["operationMetrics"]


def read(spark, path):
    return spark.read.format("delta").load(path)


def test_same_content_newer_is_not_updated(spark, tmp_path):
    """A newer fixture copy with identical amounts must not replace the official row."""

    path = str(tmp_path / "silver")
    merge_silver(spark, trip(spark, "official_tlc", 3.0, "2026-01-01 10:00:00", "b1"), path)
    merge_silver(spark, trip(spark, "generated_fixture", 3.0, "2026-01-01 11:00:00", "b2"), path)

    assert last_metrics(spark, path)["numTargetRowsUpdated"] == "0"
    assert read(spark, path).first()["record_source"] == "official_tlc"


def test_newer_changed_tip_is_updated(spark, tmp_path):
    path = str(tmp_path / "silver")
    merge_silver(spark, trip(spark, "official_tlc", 3.0, "2026-01-01 10:00:00", "b1"), path)
    merge_silver(spark, trip(spark, "official_tlc", 4.0, "2026-01-01 11:00:00", "b2"), path)

    assert last_metrics(spark, path)["numTargetRowsUpdated"] == "1"
    assert read(spark, path).first()["tip_amount"] == 4.0


def test_older_change_is_ignored(spark, tmp_path):
    """Replaying an older (different) version must not move the tip backwards."""

    path = str(tmp_path / "silver")
    merge_silver(spark, trip(spark, "official_tlc", 4.0, "2026-01-01 11:00:00", "b2"), path)
    merge_silver(spark, trip(spark, "official_tlc", 2.0, "2026-01-01 10:00:00", "b1"), path)

    assert last_metrics(spark, path)["numTargetRowsUpdated"] == "0"
    assert read(spark, path).first()["tip_amount"] == 4.0


def test_new_trip_is_inserted(spark, tmp_path):
    path = str(tmp_path / "silver")
    merge_silver(spark, trip(spark, "official_tlc", 3.0, "2026-01-01 10:00:00", "b1"), path)
    merge_silver(
        spark, trip(spark, "cdc_demo", 5.0, "2026-01-01 11:00:00", "b2", trip_id="Y"), path
    )

    metrics = last_metrics(spark, path)
    assert metrics["numTargetRowsInserted"] == "1"
    assert metrics["numTargetRowsUpdated"] == "0"
    assert sorted(row["trip_id"] for row in read(spark, path).collect()) == ["X", "Y"]


def test_schema_evolution_adds_column_and_restores_flag(spark, tmp_path):
    path = str(tmp_path / "silver")
    merge_silver(spark, trip(spark, "official_tlc", 3.0, "2026-01-01 10:00:00", "b1"), path)
    evolved = trip(spark, "cdc_demo", 4.0, "2026-01-01 11:00:00", "b2").withColumn(
        "surcharge_fee", F.lit(1.5)
    )
    merge_silver(spark, evolved, path)

    row = read(spark, path).first()
    assert row["surcharge_fee"] == 1.5
    assert row["tip_amount"] == 4.0
    # autoMerge is scoped to the MERGE and must not leak into the session.
    assert spark.conf.get(AUTO_MERGE_CONF, "false") == "false"
