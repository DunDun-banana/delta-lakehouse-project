# Task 1 - Bronze Ingestion

## Objective

Land every NYC TLC monthly Parquet file of 2025, plus a generated dirty
fixture, into one append-only Delta table **without changing or dropping any
value**, adding lineage so every row can be traced to its source batch.

## Logic

1. `prepare_dirty_parquet` samples 5,000 rows of the November file, adds
   synthetic GPS coordinates and a `trip_id`, injects corruptions and appends
   duplicates; output `data/landing/dirty_test.parquet` (5,218 rows).
2. `bronze_ingestion.run_bronze` treats each input as one batch
   (`ingest_batch_id` = file stem). For each batch:
   - skip it if Bronze already contains that `ingest_batch_id` (idempotent rerun),
   - `normalize_input`: add `trip_id` (if absent) and `record_source = official_tlc`,
     cast integer columns to `long`, amounts to `double`, dates to `string`,
   - `add_bronze_metadata`: `source_file`, `source_path`, `ingest_batch_id`,
     `ingested_at`, `raw_record_hash` (SHA-256 of all business columns),
   - append with `mergeSchema = true` (the fixture adds GPS columns),
   - read the written row count from the commit metric `numOutputRows`.

Fixture corruptions (by fixture row id):

| Column | Every Nth row | Value |
|---|---|---|
| `tpep_pickup_datetime` | 13 | `not-a-date` |
| `tpep_dropoff_datetime` | 29 | `2025-99-99 25:61:00` |
| `PULocationID` / `DOLocationID` | 17 / 19 | NULL |
| pickup / dropoff GPS | 7 / 11 | NULL |
| `fare_amount` | 101 | -1.0 |
| duplicates | 23 | exact copy, at most 5% of the sample |

## Input

- `data/source/yellow_tripdata_2025-01.parquet` ... `-12.parquet`
- `data/landing/dirty_test.parquet` (created if missing by `lakehouse_pipeline.py`)

## Output

`data/bronze/taxi_trips` - one Delta commit per batch. Columns: the TLC
columns, `trip_id`, `record_source`, `source_file`, `source_path`,
`ingest_batch_id`, `ingested_at`, `raw_record_hash`, and the four synthetic
GPS columns (NULL for official rows).

## Business rules

- Append only: no update, delete, MERGE or deduplication in Bronze.
- Dates stay strings so malformed values survive until Silver.
- Type widening only (`int` -> `long`, numeric -> `double`); monthly files
  disagree on integer widths and Delta cannot store two types in one column.
- `trip_id` = SHA-256 of `VendorID || PULocationID || DOLocationID ||
  trip_distance || pickup || dropoff`, NULL -> empty string
  (`src/common/ids.py`). TLC data has no trip identifier, so this is a
  synthetic key: two genuinely different trips with identical values in all
  six fields would share an id.

## Test cases

`tests/test_bronze.py`

| Test | Checks |
|---|---|
| `test_trip_id_is_deterministic_sha256` | same input -> same 64-char id |
| `test_trip_id_changes_with_identity_columns` | one second difference -> different id |
| `test_trip_id_ignores_non_identity_columns` | fare is not part of the id |
| `test_null_in_different_positions_does_not_collide` | (NULL, 1) != (1, NULL) |
| `test_normalize_keeps_dirty_values_and_widens_types` | `not-a-date` and fare -1 kept, `bigint`, `official_tlc` |
| `test_normalize_keeps_existing_fixture_lineage` | fixture `trip_id` / `record_source` untouched |

## Expected result

13 batches and 13 commits; Bronze row count equals the sum of the inputs;
dirty rows are present unchanged; a second run appends nothing.

## Actual result

| Batch | Rows | Batch | Rows |
|---|---:|---|---:|
| 2025-01 | 3,475,226 | 2025-07 | 3,898,963 |
| 2025-02 | 3,577,543 | 2025-08 | 3,574,091 |
| 2025-03 | 4,145,257 | 2025-09 | 4,251,015 |
| 2025-04 | 3,970,553 | 2025-10 | 4,428,699 |
| 2025-05 | 4,591,845 | 2025-11 | 4,181,444 |
| 2025-06 | 4,322,960 | 2025-12 | 4,305,006 |
| dirty_test | 5,218 | **total** | **48,727,820** |

- 13 batches (Bronze v0-v12), 58 data files. A rerun skipped all 13 batches.
- v13-v15 are the three one-row CDC demo events of Task 2.
- Dirty batch content (from the demo notebook): 402 bad pickup dates,
  181 bad dropoff dates, 308 / 276 NULL pickup / dropoff zones,
  747 NULL pickup GPS, 119 negative fares, 218 duplicate rows.
- `PULocationID` min/max over the v0 files is 1-265 (all taxi zones).

## Code link

- [`src/bronze/prepare_dirty_parquet.py`](../src/bronze/prepare_dirty_parquet.py)
- [`src/bronze/bronze_ingestion.py`](../src/bronze/bronze_ingestion.py)
- [`src/common/ids.py`](../src/common/ids.py)

```bash
python -m src.bronze.prepare_dirty_parquet          # November sample by default
python -m src.bronze.bronze_ingestion               # all months + fixture
python -m src.bronze.bronze_ingestion --input data/source/yellow_tripdata_2025-12.parquet
```

## Notes

- The skip check (`already_ingested`) makes reruns safe, but a file renamed
  to a new stem would be appended again.
- `ingested_at` is processing time, not an event time from TLC; Silver uses
  it only to order re-ingested copies of the same trip.
- Changing the `trip_id` formula would break the MERGE key of the existing Silver table.
