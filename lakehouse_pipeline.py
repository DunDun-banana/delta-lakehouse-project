"""End-to-end orchestration: Bronze -> Silver -> Gold (+ optional Task 3 audit).

All stages share one Spark session and run in order; the first failing stage
stops the run. Every stage is idempotent on the existing tables:

* bronze - creates the dirty fixture if missing, then appends only batches
  whose ingest_batch_id is not in Bronze yet,
* silver - processes only Bronze commits not yet in the streaming checkpoint,
* gold   - full recompute of the small Gold table from the latest Silver version,
* audit  - read-only Task 3 verification (only with --audit).

Run from the project root:
    python lakehouse_pipeline.py
    python lakehouse_pipeline.py --stages silver gold --audit
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Callable

from pyspark.sql import SparkSession

from src.common.config import DIRTY_FIXTURE, SILVER_PATH

STAGES = ("bronze", "silver", "gold")


def run_bronze_layer(spark: SparkSession) -> dict:
    """Task 1: prepare the dirty fixture when missing, then ingest Bronze."""

    from src.bronze.bronze_ingestion import run_bronze
    from src.bronze.prepare_dirty_parquet import prepare_fixture

    if not DIRTY_FIXTURE.exists():
        prepare_fixture(spark)
    return run_bronze(spark)


def run_silver_layer(spark: SparkSession) -> dict:
    """Task 2: incremental Silver CDC (availableNow trigger)."""

    from src.silver.silver_pipeline import run_silver

    return run_silver(spark)


def run_gold_layer(spark: SparkSession) -> dict:
    """Task 4: Gold zone x hour metrics from the latest Silver version."""

    from src.gold.gold_aggregation import run_gold

    return run_gold(spark)


def run_audit_layer(spark: SparkSession) -> dict:
    """Task 3: read-only time travel and _delta_log audit of Silver."""

    from src.audit.time_travel import run_audit

    evidence = run_audit(spark, str(SILVER_PATH))
    if evidence["verification"] != "PASS":
        raise RuntimeError(f"Audit result: {evidence['verification']} {evidence['checks']}")
    return {"verification": evidence["verification"], "runtime_seconds": evidence["runtime_seconds"]}


RUNNERS: dict[str, Callable[[SparkSession], dict]] = {
    "bronze": run_bronze_layer,
    "silver": run_silver_layer,
    "gold": run_gold_layer,
    "audit": run_audit_layer,
}


def run_pipeline(spark: SparkSession, stages: list[str]) -> list[dict]:
    """Run ``stages`` in order; stop at the first failure. Returns one row per stage."""

    results = []
    for stage in stages:
        start = time.perf_counter()
        try:
            RUNNERS[stage](spark)
            status, error = "OK", ""
        except Exception as exc:  # report the failing stage, then stop
            logging.getLogger("pipeline").exception("Stage %s failed", stage)
            status, error = "FAILED", f"{type(exc).__name__}: {exc}"
        results.append(
            {
                "stage": stage,
                "status": status,
                "seconds": round(time.perf_counter() - start, 1),
                "error": error,
            }
        )
        if status != "OK":
            break
    return results


def print_summary(results: list[dict], requested: list[str]) -> None:
    done = {row["stage"] for row in results}
    print(f"\n{'stage':<8}{'status':<10}{'seconds':>9}  error")
    for row in results:
        print(f"{row['stage']:<8}{row['status']:<10}{row['seconds']:>9.1f}  {row['error'][:120]}")
    for stage in requested:
        if stage not in done:
            print(f"{stage:<8}{'SKIPPED':<10}{'-':>9}")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=STAGES,
        default=list(STAGES),
        help="Stages to run, always executed in bronze -> silver -> gold order",
    )
    parser.add_argument("--audit", action="store_true", help="Run the Task 3 audit at the end")
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    from src.common.spark import create_spark

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stages = [stage for stage in STAGES if stage in args.stages]
    if args.audit:
        stages.append("audit")

    spark = create_spark("lakehouse-pipeline", args.master)
    try:
        results = run_pipeline(spark, stages)
    finally:
        spark.stop()
    print_summary(results, stages)
    return 0 if all(row["status"] == "OK" for row in results) else 1


if __name__ == "__main__":
    sys.exit(main())
