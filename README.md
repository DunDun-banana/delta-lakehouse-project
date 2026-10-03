# Delta Lakehouse - NYC Yellow Taxi 2025

A local medallion lakehouse (Bronze -> Silver -> Gold) on **Delta Lake 2.4 /
Spark 3.4**, built on the 12 monthly NYC TLC Yellow Taxi files of 2025
(48.7 M trips). It demonstrates:

- append-only Bronze ingestion that keeps dirty data,
- incremental Silver cleaning + CDC with Delta streaming and `MERGE`,
- schema evolution, time travel and `_delta_log` audit,
- a Gold analytics table, and `OPTIMIZE` / `Z-ORDER` benchmarking.

The full write-up is in [REPORT.md](REPORT.md).

## Results at a glance

| Area | Result |
|---|---|
| Bronze | 13 batches (12 months + dirty fixture), 48,727,820 rows, 58 files; a rerun skipped all 13 batches |
| Silver | 45,849,822 rows after the initial load; 2,874,740 rejected, 3,258 deduplicated, 0 duplicate `trip_id` |
| CDC demo | UPDATE v44 (fare 26.8 -> 28.8), INSERT v45 (45,849,823 rows), schema evolution v46 (`surcharge_fee` = 1.5) |
| Time travel audit | PASS in 11.6 s, read-only, `versionAsOf 0` = 1,276,635 rows |
| Gold | 6,182 (zone, hour) groups from Silver v46; 45,831,981 trips used, 17,842 outliers filtered |
| OPTIMIZE | 689 -> 9 files; Q1 one-zone query 4.744 s -> 2.794 s (compaction) -> 0.612 s (Z-ORDER, 1 of 9 files read) |

## Repository structure

```text
lakehouse_pipeline.py        End-to-end orchestration (bronze, silver, gold, optional audit)
optimization_benchmark.py    Task 4 OPTIMIZE / Z-ORDER benchmark (writes Silver: OPTIMIZE)
src/
  common/                    config.py (paths), spark.py (only Spark factory), ids.py (trip_id), delta_utils.py
  bronze/                    prepare_dirty_parquet.py, bronze_ingestion.py          (Task 1)
  silver/                    silver_cleaning.py, silver_merge.py, schema_evolution.py, silver_pipeline.py (Task 2)
  audit/                     time_travel.py                                         (Task 3)
  gold/                      gold_aggregation.py                                    (Task 4)
  optimization/              data_skipping.py (per-file min/max replay of _delta_log) (Task 4)
scripts/                     Demo and read-only helper scripts (python -m scripts.<name>)
tests/                       pytest suite (tiny temporary Delta tables only)
notebooks/lakehouse_demo.ipynb  Read-only live demo
docs/                        Per-task documentation, runbook and evidence JSON
data/                        Local data and Delta tables (not in Git, see data/README.md)
```

## Environment

| Component | Version |
|---|---|
| Python | 3.10 or 3.11 (3.12+ is not supported by PySpark 3.4.1) |
| Java | 8, 11 or 17 |
| PySpark | 3.4.1 |
| delta-spark | 2.4.0 |

Results in this repository were measured on a macOS laptop with 8 cores and an
external SSD, Python 3.11, Spark 3.4.1 and Delta 2.4.0.

```bash
python3.11 -m venv .venv311
source .venv311/bin/activate
python -m pip install -r requirements.txt
```

Spark settings can be tuned with environment variables:
`SPARK_DRIVER_MEMORY` (default `4g`) and `SPARK_SHUFFLE_PARTITIONS` (default `16`).

## How to run

All commands run from the repository root with the virtual environment active.

```bash
# 1. Put yellow_tripdata_2025-01.parquet ... -12.parquet into data/source/

# 2. Full pipeline (each stage is idempotent and skips work already done)
python lakehouse_pipeline.py                      # bronze -> silver -> gold
python lakehouse_pipeline.py --stages silver gold # selected stages
python lakehouse_pipeline.py --audit              # + read-only Task 3 audit

# 3. CDC demo (writes one event into Bronze, then process it)
python -m scripts.demo_silver_cdc --mode update && python -m src.silver.silver_pipeline --once
python -m scripts.demo_silver_cdc --mode insert && python -m src.silver.silver_pipeline --once
python -m scripts.demo_silver_cdc --mode schema && python -m src.silver.silver_pipeline --once

# 4. Time travel audit (read-only)
python -m src.audit.time_travel --output logs/task3_audit.json

# 5. Gold and OPTIMIZE / Z-ORDER benchmark (the benchmark commits OPTIMIZE to Silver)
python -m src.gold.gold_aggregation
python optimization_benchmark.py

# Tests (about 1 minute; tiny temporary tables only)
python -m pytest -q
```

Each layer can also be run on its own:
`python -m src.bronze.prepare_dirty_parquet`, `python -m src.bronze.bronze_ingestion`,
`python -m src.silver.silver_pipeline --once`, `python -m src.gold.gold_aggregation`.

## Scripts

| Script | Writes? | Purpose |
|---|---|---|
| `scripts.demo_silver_cdc --mode update/insert/schema` | Bronze | Append one synthetic CDC event |
| `scripts.check_silver` | no | Bronze/Silver reconciliation, duplicate check, reasons (~5 min) |
| `scripts.peek_cdc --trip ID --before N --after M` | no | One trip at two versions + commit metrics |
| `scripts.profile_outliers` | no | Percentiles and threshold impact used for the Gold filters |
| `scripts.peek_zero_duration` | no | Zero-duration trips by VendorID |
| `scripts.peek_gold` | no | Top Gold groups and zone 132 by hour |
| `scripts.export_silver_history` | `logs/` | Silver history + batch audit to JSON |
| `scripts.show_file_stats --versions 46 48 --value 132` | no | Per-file min/max from `_delta_log` (no Spark) |

## Consistency rules

- The 12 official source Parquet files are committed on purpose so the project
  runs right after cloning; never commit generated Delta tables, checkpoints,
  the dirty fixture or logs.
- `data/checkpoints/silver_taxi` belongs to `data/silver/taxi_trips`,
  `rejected_records` and `batch_audit`. Delete or restore these **four
  together**, never one alone; otherwise Silver is either reprocessed or
  silently skips Bronze commits.
- Do **not** run `VACUUM` before the demo: it deletes the files of older
  versions, which breaks time travel to the baseline (v46) and earlier CDC versions.
- Do not run Bronze ingestion against a table built with a different
  `trip_id` formula; `trip_id` is the MERGE key of Silver.
- The notebook and `scripts.*` marked "no" are read-only and safe during a demo.

## Documentation

| Document | Content |
|---|---|
| [REPORT.md](REPORT.md) | Part A theory and Part B implementation report |
| [docs/architecture.md](docs/architecture.md) | Architecture, data flow, tables |
| [docs/task1_bronze.md](docs/task1_bronze.md) | Task 1 - Bronze ingestion |
| [docs/task2_silver_cdc.md](docs/task2_silver_cdc.md) | Task 2 - Silver cleaning and CDC |
| [docs/task3_time_travel.md](docs/task3_time_travel.md) | Task 3 - Time travel and audit |
| [docs/task4_gold_optimization.md](docs/task4_gold_optimization.md) | Task 4 - Gold and optimization |
| [docs/demo_runbook.md](docs/demo_runbook.md) | 15-minute live demo runbook |
| [docs/evidence/README.md](docs/evidence/README.md) | Evidence JSON files |
| [data/README.md](data/README.md) | Local data layout |
