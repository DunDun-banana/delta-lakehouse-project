# Task 4 - Gold Layer and Storage Optimization

## Objective

1. Build a Gold table with business metrics per pickup zone and hour of day.
2. Fix the small-file layout of Silver with `OPTIMIZE` and cluster it with
   `Z-ORDER BY (PULocationID)`, and measure the effect on query time and on
   the number of files read.

## Logic

**Gold** (`src/gold/gold_aggregation.py`): read one pinned Silver version
(`versionAsOf`, default latest), apply analytics filters, group by
`PULocationID` and `hour(tpep_pickup_datetime)`, and overwrite
`data/gold/zone_hourly_metrics` with the `silver_version` it was built from.

**Benchmark** (`optimization_benchmark.py`): three states of the same table
kept side by side by time travel:

| State | Version | How it is made |
|---|---|---|
| A_baseline | last version before OPTIMIZE (v46) | as produced by Silver MERGEs |
| B_compaction | v47 | `OPTIMIZE` (bin-packing to ~1 GB files) |
| C_zorder | v48 | `OPTIMIZE ZORDER BY (PULocationID)` |

Existing OPTIMIZE commits are reused, so a rerun does not optimize again.
For each state and query: 1 warm-up run + 5 measured runs, median reported.
`files_to_read` is recomputed independently from the `_delta_log` stats by
`src/optimization/data_skipping.py` (replay add/remove, keep files whose
[min, max] overlaps the filter).

| Query | Filter | Purpose |
|---|---|---|
| Q1_single_zone | `PULocationID = 132` (JFK) | point filter on the Z-ORDER column |
| Q2_zone_range | `PULocationID BETWEEN 100 AND 120` | range filter |
| Q3_full_gold | none (full Gold aggregation) | control query, no skipping possible |

## Input

`data/silver/taxi_trips` (Silver v46: 45,849,823 rows, 689 files, 9.05 GB).

## Output

- `data/gold/zone_hourly_metrics`: `PULocationID, pickup_hour, trip_count,
  avg_fare, card_trip_count, tip_pct, avg_driver_earnings,
  total_driver_earnings, silver_version, computed_at`.
- Silver commits v47 (compaction) and v48 (Z-ORDER).
- [`docs/evidence/task4_benchmark_2026-10-03.json`](evidence/task4_benchmark_2026-10-03.json).

## Business rules

| Metric | Definition |
|---|---|
| `avg_fare` | mean `fare_amount` (metered fare only) |
| `tip_pct` | `sum(tip) / sum(fare) * 100` over **card trips only** (`payment_type = 1`); TLC records cash tips as 0 |
| driver earnings | `fare_amount + extra + tip_amount` (taxes, surcharges and tolls are pass-through) |
| `card_trip_count` | trips with `payment_type = 1` |

Analytics filters (Silver itself keeps these rows), chosen from
`scripts/profile_outliers.py`:

| Column | p50 | p99 | p99.9 | p99.99 | max | Rule | Trips removed |
|---|---:|---:|---:|---:|---:|---|---:|
| fare | 14.34 | 80.70 | 150.00 | 322.22 | 863,372.12 | `<= 500` | 844 |
| distance (miles) | 1.82 | 19.60 | 29.86 | 61.09 | 397,994.37 | `<= 100` | 2,436 |
| duration (min) | 13.37 | 71.35 | 114.90 | 1,427.58 | 14,880.77 | `<= 6 h` | 14,780 |
| pickup year | | | | | | `= 2025` | 29 |

Rules overlap, so the total removed (17,842) is less than the sum.
Zero-duration trips (543,428) are **kept**: 535,901 of them come from
VendorID 7, which records every trip with zero duration but normal fares and
distances (`scripts/peek_zero_duration.py`).

## Test cases

`tests/test_gold.py`: outliers (fare > 500, distance > 100, duration > 6 h,
year 2024) removed and duration 0 kept; trip count, card count and average
fare; `tip_pct` from card trips only as sum/sum; earnings = fare + extra + tip.
Timestamps are given as strings and parsed by Spark in UTC, so the tests do
not depend on the machine time zone.

`tests/test_data_skipping.py`: synthetic `_delta_log` replay, 3 -> 1 files
for a point filter after Z-ORDER, a file without stats is always read,
missing commit detected.

## Expected result

Gold has one row per (zone, hour) with plausible averages; compaction cuts
the file count and per-file overhead; Z-ORDER lets a zone filter skip most files.

## Actual result

Gold (built from Silver v46 in about 20 s): 45,831,981 trips used,
17,842 filtered, 6,182 groups. Top group: JFK (zone 132) at 16:00 with
147,946 trips, average fare 67.26, tip 19.5%, average driver earnings 80.99,
total 11,981,497.

OPTIMIZE:

| Step | Version | Files | Time | Size after |
|---|---|---|---:|---:|
| Compaction | v47 | 689 -> 9 | 223.2 s | 9.77 GB |
| Z-ORDER | v48 | 9 -> 9 | 495.4 s | 9.67 GB |

Benchmark (median of 5 runs, `local[*]`, 8 cores, driver 8 g):

| State | Q1 files | Q1 s | Q2 files | Q2 s | Q3 files | Q3 s |
|---|---:|---:|---:|---:|---:|---:|
| A baseline | 688/689 | 4.744 | 688/689 | 5.821 | 689/689 | 16.527 |
| B compaction | 9/9 | 2.794 (1.70x) | 9/9 | 3.020 (1.93x) | 9/9 | 12.571 (1.31x) |
| C Z-ORDER | 1/9 | 0.612 (7.75x) | 2/9 | 0.931 (6.25x) | 9/9 | 8.963 (1.84x) |

- Compaction does not skip anything (every file still covers zones 1-265) but
  removes per-file overhead: 9 large files instead of 689 small ones.
- Z-ORDER narrows each file to a zone range (average range width 29 zones
  instead of about 263), so Q1 opens 1 file and Q2 opens 2.
- Q3 reads every file in all states. It is faster on C than on B; the
  hypothesis that zone-clustered input helps the `groupBy(PULocationID, hour)`
  is **not verified**.

## Code link

- [`src/gold/gold_aggregation.py`](../src/gold/gold_aggregation.py)
- [`optimization_benchmark.py`](../optimization_benchmark.py)
- [`src/optimization/data_skipping.py`](../src/optimization/data_skipping.py)
- [`scripts/show_file_stats.py`](../scripts/show_file_stats.py)

```bash
python -m src.gold.gold_aggregation                    # latest Silver version
python -m src.gold.gold_aggregation --silver-version 46
python optimization_benchmark.py --runs 5              # commits OPTIMIZE once
python -m scripts.show_file_stats --versions 46 48 --value 132
```

## Notes

- OPTIMIZE commits only `remove` + `add` with `dataChange = false`; the data
  and Gold results are unchanged, and older versions stay readable until VACUUM.
- Do not VACUUM before the benchmark and demo are finished: states A and B
  would lose their files.
- Wall-clock times depend on the laptop and disk cache; `files_to_read` does not.
