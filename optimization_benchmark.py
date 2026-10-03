"""Task 4 - Benchmark Delta OPTIMIZE and Z-ORDER on the Silver table.

Three states of the SAME table, kept side by side with Delta time travel:
  A_baseline   - Silver before optimization (many ~13 MB files, unordered)
  B_compaction - OPTIMIZE (bin-packing into ~1 GB files, still unordered)
  C_zorder     - OPTIMIZE ZORDER BY (PULocationID)

For every (state, query) the script records:
  - files_to_read: files Delta must open after min/max data skipping on
    PULocationID, recomputed from the _delta_log stats (independent of disk speed)
  - wall-clock time: 1 warm-up run + N measured runs, median reported

Do NOT run VACUUM before the benchmark is finished: it deletes the files of
states A and B, and time travel to those versions stops working.

Run from the project root:
    python optimization_benchmark.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import time
from datetime import date
from importlib.metadata import version as package_version
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import EVIDENCE_DIR, SILVER_PATH
from src.gold.gold_aggregation import build_gold
from src.optimization.data_skipping import active_files, files_to_read

LOG = logging.getLogger("benchmark")

ZORDER_COLUMN = "PULocationID"

# name -> (description, PULocationID range used for skipping or None, query builder)
QUERIES = {
    "Q1_single_zone": (
        "Stats for one pickup zone (132 = JFK)",
        (132, 132),
        lambda df: df.filter(F.col(ZORDER_COLUMN) == 132).agg(
            F.count("*"), F.avg("fare_amount"), F.avg("tip_amount")
        ),
    ),
    "Q2_zone_range": (
        "Stats for pickup zones 100-120",
        (100, 120),
        lambda df: df.filter(F.col(ZORDER_COLUMN).between(100, 120)).agg(
            F.count("*"), F.avg("fare_amount"), F.avg("tip_amount")
        ),
    ),
    "Q3_full_gold": (
        "Full Gold aggregation without filter (control query)",
        None,
        build_gold,
    ),
}


# --------------------------------------------------------------------------- #
# Preparing states B and C (idempotent: reuses OPTIMIZE commits already present)
# --------------------------------------------------------------------------- #
def find_optimize_versions(table: DeltaTable) -> tuple[int | None, int | None]:
    """Return the latest (compaction version, zorder version) found in history."""

    compaction = zorder = None
    for row in table.history().collect():  # newest first
        if row["operation"] != "OPTIMIZE":
            continue
        zorder_columns = json.loads((row["operationParameters"] or {}).get("zOrderBy", "[]"))
        if ZORDER_COLUMN in zorder_columns:
            zorder = row["version"] if zorder is None else zorder
        elif not zorder_columns:
            compaction = row["version"] if compaction is None else compaction
    return compaction, zorder


def run_optimize(table: DeltaTable, zorder: bool) -> dict:
    """Run OPTIMIZE (optionally Z-ORDER) and return its commit metrics."""

    builder = table.optimize()
    start = time.perf_counter()
    if zorder:
        builder.executeZOrderBy(ZORDER_COLUMN)
    else:
        builder.executeCompaction()
    seconds = time.perf_counter() - start

    commit = table.history(1).first()
    metrics = commit["operationMetrics"] or {}
    result = {
        "version": commit["version"],
        "zorder_by": [ZORDER_COLUMN] if zorder else [],
        "seconds": round(seconds, 1),
        "files_removed": int(metrics.get("numRemovedFiles", 0)),
        "files_added": int(metrics.get("numAddedFiles", 0)),
        "bytes_added": int(metrics.get("numAddedBytes", 0)),
    }
    LOG.info(json.dumps({"event": "optimize", **result}))
    return result


def prepare_states(spark: SparkSession, path: str) -> dict:
    """Make sure states B and C exist, then return the version of each state."""

    table = DeltaTable.forPath(spark, path)
    compaction, zorder = find_optimize_versions(table)
    runs = {}
    if compaction is None:
        if zorder is not None:
            raise RuntimeError("Found Z-ORDER without a prior compaction; cannot build state B")
        runs["compaction"] = run_optimize(table, zorder=False)
        compaction = runs["compaction"]["version"]
    if zorder is None:
        runs["zorder"] = run_optimize(table, zorder=True)
        zorder = runs["zorder"]["version"]
    versions = {"A_baseline": compaction - 1, "B_compaction": compaction, "C_zorder": zorder}
    LOG.info(json.dumps({"event": "states", **versions}))
    return {"versions": versions, "optimize_runs": runs}


# --------------------------------------------------------------------------- #
# Measuring
# --------------------------------------------------------------------------- #
def time_query(spark: SparkSession, path: str, version: int, build, runs: int) -> list[float]:
    """1 warm-up run (discarded) followed by `runs` measured runs, in seconds."""

    def once() -> float:
        df = spark.read.format("delta").option("versionAsOf", version).load(path)
        start = time.perf_counter()
        build(df).collect()
        return time.perf_counter() - start

    once()
    return [once() for _ in range(runs)]


def run_benchmark(spark: SparkSession, path: str, versions: dict, runs: int) -> list[dict]:
    """Measure every query on every state; files_to_read comes from the log stats."""

    results = []
    for state, version in versions.items():
        files = active_files(path, version)
        for name, (description, value_range, build) in QUERIES.items():
            times = time_query(spark, path, version, build, runs)
            row = {
                "state": state,
                "version": version,
                "query": name,
                "description": description,
                "files_total": len(files),
                "files_to_read": files_to_read(files, ZORDER_COLUMN, value_range),
                "runs_s": [round(t, 3) for t in times],
                "median_s": round(statistics.median(times), 3),
            }
            LOG.info(json.dumps({"event": "measured", **row}))
            results.append(row)
    return results


def compare_results(results: list[dict]) -> None:
    """Add speedup vs. baseline to every row and print a summary table."""

    baseline = {r["query"]: r["median_s"] for r in results if r["state"] == "A_baseline"}
    print(f"\n{'state':<14}{'query':<17}{'files read':>14}{'median (s)':>13}{'speedup':>10}")
    for r in results:
        r["speedup_vs_A"] = round(baseline[r["query"]] / r["median_s"], 2)
        files = f"{r['files_to_read']}/{r['files_total']}"
        print(f"{r['state']:<14}{r['query']:<17}{files:>14}{r['median_s']:>13.3f}{r['speedup_vs_A']:>9.2f}x")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--silver", default=str(SILVER_PATH))
    parser.add_argument("--runs", type=int, default=5, help="Measured runs per query (after 1 warm-up)")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument(
        "--output", default=str(EVIDENCE_DIR / f"task4_benchmark_{date.today()}.json")
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    from src.common.spark import create_spark

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = create_spark("optimization-benchmark", args.master)
    try:
        states = prepare_states(spark, args.silver)
        results = run_benchmark(spark, args.silver, states["versions"], args.runs)
        compare_results(results)

        evidence = {
            "date": str(date.today()),
            "environment": {
                "spark": spark.version,
                "delta_spark": package_version("delta-spark"),
                "master": args.master,
                "cpu_count": os.cpu_count(),
                "driver_memory": spark.conf.get("spark.driver.memory", "default"),
            },
            "silver_path": args.silver,
            "zorder_column": ZORDER_COLUMN,
            "warmup_runs": 1,
            "measured_runs": args.runs,
            **states,
            "results": results,
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(evidence, indent=2))
        print(f"\nEvidence JSON: {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()