"""The single SparkSession factory used by the whole project."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.common.config import SPARK_TMP_DIR


def create_spark(app_name: str = "delta-lakehouse", master: str = "local[*]") -> SparkSession:
    """Create (or reuse) a local SparkSession configured for Delta Lake 2.4.

    Configuration choices:

    * Delta SQL extension + DeltaCatalog: required for MERGE, OPTIMIZE,
      time travel and ``DeltaTable`` APIs.
    * ``spark.sql.session.timeZone = UTC``: TLC timestamps carry no zone, so a
      fixed session zone keeps parsing and ``hour()`` identical on every laptop.
    * ``spark.sql.shuffle.partitions`` from ``SPARK_SHUFFLE_PARTITIONS``
      (default 16): the Spark default of 200 creates tiny tasks and tiny files
      on a laptop.
    * ``spark.driver.memory`` from ``SPARK_DRIVER_MEMORY`` (default 4g): in
      local mode the driver JVM also runs every task.
    * ``spark.sql.ansi.enabled = false``: malformed dates such as
      ``"not-a-date"`` must parse to NULL so Silver can reject them with a
      reason code instead of failing the whole microbatch.
    * ``spark.local.dir = data/tmp``: shuffle and spill files go to the same
      (external) disk as the data instead of the system drive.
    * ``configure_spark_with_delta_pip``: adds the matching Delta JAR from the
      installed ``delta-spark`` package.

    Schema auto-merge is deliberately NOT enabled globally; MERGE statements
    that need it use ``src.common.delta_utils.schema_auto_merge``.
    """

    SPARK_TMP_DIR.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.local.dir", str(SPARK_TMP_DIR))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
