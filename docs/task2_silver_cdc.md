# Task 2 - Silver Cleaning and CDC

## Objective

Read Bronze incrementally, reject invalid trips with explicit reason codes,
keep exactly one row per `trip_id`, and apply changes (CDC) to Silver with
Delta `MERGE`: UPDATE an existing trip when a newer, different version
arrives, INSERT new trips, and evolve the schema when a new column appears.

## Logic

```text
Bronze (Delta) --readStream, maxBytesPerTrigger 128m--> foreachBatch(process_batch)
   validate_and_split  -> valid (parsed timestamps + business_hash) / rejected (+ reasons)
   deduplicate(valid)  -> one winner per trip_id (newest ingested_at, batch id, raw hash)
   merge_silver        -> MERGE ON t.trip_id = s.trip_id
                            WHEN MATCHED AND UPDATE_CONDITION THEN UPDATE *
                            WHEN NOT MATCHED THEN INSERT *
   merge_rejected      -> insert-only MERGE on (ingest_batch_id, raw_record_hash)
   _upsert_audit       -> one batch_audit row per microbatch (MERGE on batch_id)
checkpoint commit   -> written by Spark only after foreachBatch succeeds
```

```text
NEWER            = s.ingested_at > t.ingested_at
                   OR (equal AND s.ingest_batch_id > t.ingest_batch_id)
                   OR (equal AND equal AND s.raw_record_hash > t.raw_record_hash)
UPDATE_CONDITION = (NEWER) AND NOT (s.business_hash <=> t.business_hash)
```

`business_hash` = SHA-256 of the CDC columns (`fare_amount`, `tip_amount`,
`total_amount`, `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`,
`congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee`,
`passenger_count`, `payment_type`, `surcharge_fee`). Lineage and synthetic GPS
are excluded, so a re-ingested copy with the same amounts is not an update.

Schema evolution: `check_evolution` allows only new columns (type changes
and missing protected columns raise), and both MERGEs run inside
`schema_auto_merge`, which enables
`spark.databricks.delta.schema.autoMerge.enabled` for that MERGE only
(delta-spark 2.4 has no `withSchemaEvolution()`).

## Input

- `data/bronze/taxi_trips` as a Delta stream; checkpoint `data/checkpoints/silver_taxi`
  (query name `silver_taxi_cdc`).
- CDC demo events from `python -m scripts.demo_silver_cdc --mode update|insert|schema`.

## Output

| Table | Content |
|---|---|
| `data/silver/taxi_trips` | valid trips, timestamps parsed, one row per `trip_id`, `business_hash` |
| `data/silver/rejected_records` | rejected rows with raw date strings, `rejection_reasons`, `rejected_at` |
| `data/silver/batch_audit` | `batch_id, incoming, valid, rejected, winners, processed_at, silver_version_before, silver_version_after, silver_operation, silver_operation_metrics, silver_commit_timestamp` |

## Business rules

| Reason code | Rejected when |
|---|---|
| `missing_trip_id` | `trip_id` NULL or blank |
| `invalid_datetime` | pickup or dropoff does not parse as `yyyy-MM-dd HH:mm:ss` |
| `dropoff_before_pickup` | dropoff < pickup |
| `invalid_location` | a zone is NULL or <= 0 |
| `invalid_fare` | fare NULL, NaN or <= 0 (NaN needs its own check: `NaN <= 0` is false) |
| `invalid_distance` | distance NULL or < 0 |
| `invalid_total_amount` | total NULL or <= 0 |
| `invalid_passenger_count` | passenger_count < 0 (NULL is kept: TLC leaves it NULL for many trips) |
| `invalid_coordinates` | only for `record_source = generated_fixture`: GPS NULL or outside lat 40-41.5 / lon -74.5 to -73 (official files have no GPS) |

- A row collects **all** failing reasons.
- Deduplication uses `row_number()` over a deterministic order, not
  `dropDuplicates`, which keeps an arbitrary row.
- An older or identical row never overwrites Silver; replays are no-ops.

## Test cases

`tests/test_silver_cleaning.py`, `tests/test_merge.py`

| Test | Checks |
|---|---|
| `test_valid_and_rejected_with_multiple_reasons` | bad date, multi-reason row, dropoff before pickup, NaN fare |
| `test_rejected_rows_keep_raw_dates_and_valid_rows_get_timestamps` | raw strings kept for audit |
| `test_null_passenger_count_is_kept` | NULL kept, negative rejected |
| `test_gps_is_checked_only_for_generated_fixture` | official rows without GPS stay valid |
| `test_business_hash_ignores_lineage_and_gps` | same amounts -> same hash; tip change -> new hash |
| `test_deduplicate_keeps_newest_ingestion` / `..._breaks_timestamp_ties_by_batch_id` | deterministic winner |
| `test_same_content_newer_is_not_updated` | the v43 fixture case: no overwrite |
| `test_newer_changed_tip_is_updated` | real CDC update |
| `test_older_change_is_ignored` | replay of an older version |
| `test_new_trip_is_inserted` | INSERT path |
| `test_schema_evolution_adds_column_and_restores_flag` | `surcharge_fee` added, autoMerge flag not leaked |

## Expected result

Silver has no duplicate `trip_id`, every Bronze row is either in Silver,
rejected, or explained as a duplicate, no fixture row overwrites an official
row, and the three CDC demo events produce one UPDATE, one INSERT and one
schema-evolution commit.

## Actual result

Initial load (44 microbatches):

| Metric | Value |
|---|---:|
| Silver rows | 45,849,822 |
| Rejected rows | 2,874,740 |
| Bronze rows neither in Silver nor rejected | 3,258 |
| Duplicate `trip_id` in Silver | 0 |
| `generated_fixture` rows in Silver | 0 |

The 3,258 rows are: 3,031 valid fixture rows whose `trip_id` matched an
official November trip with the same amounts (not updated), 136 duplicates
removed inside a microbatch, and 91 duplicate rejected fixture rows collapsed
by the rejected-table key. Before the `business_hash` fix, the fixture
overwrote 3,031 official rows (2,992 in an earlier team run).

Rejection reasons (a row can have several): `invalid_fare` 2,870,354,
`invalid_total_amount` 980,592, `dropoff_before_pickup` 2,235,
`invalid_coordinates` 1,105, `invalid_datetime` 544, `invalid_location` 543.

Silver versions:

| Version | Operation | Detail |
|---|---|---|
| v0 | WRITE | first microbatch, 1,276,635 rows |
| v1-v42 | MERGE | rest of the monthly data; 688 files, 9.05 GB after the load |
| v43 | MERGE | dirty fixture: 3,031 source rows, 0 updated, 0 inserted, 16 files rewritten (1,031,434 rows copied) |
| v44 | MERGE | UPDATE trip `93184dd0...9c05`: fare 26.8 -> 28.8, tip 5.86 -> 6.86; 1 remove + 1 add, 63,862 rows copied |
| v45 | MERGE | INSERT trip `e5f6221b...c287`: 45,849,823 rows; 1 add of 10 KB |
| v46 | MERGE | schema evolution: `surcharge_fee` = 1.5, fare 30.8, tip 7.86; `metaData` + remove (the v44 file) + add |

`batch_audit` totals (47 microbatches incl. the 3 CDC events): incoming
48,727,823, valid 45,852,992, rejected 2,874,831, winners 45,852,856.

Performance: MERGE scan time grows with the target (`scanTimeMs` v5 6.3 s,
v20 16.7 s, v30 27.4 s, v42 43.1 s) because each MERGE must find matches in
the whole table. The initial load took about 1 h 25 min on `local[2]` and
about 52 min on 8 cores.

## Code link

- [`src/silver/silver_pipeline.py`](../src/silver/silver_pipeline.py)
- [`src/silver/silver_cleaning.py`](../src/silver/silver_cleaning.py)
- [`src/silver/silver_merge.py`](../src/silver/silver_merge.py)
- [`src/silver/schema_evolution.py`](../src/silver/schema_evolution.py)
- [`scripts/demo_silver_cdc.py`](../scripts/demo_silver_cdc.py)

```bash
python -m src.silver.silver_pipeline --once                      # process pending Bronze commits
python -m scripts.demo_silver_cdc --mode update && python -m src.silver.silver_pipeline --once
python -m scripts.check_silver                                   # read-only reconciliation (~5 min)
```

## Notes

- Delta 2.4 rewrites every file that has an ON-clause match, even when the
  UPDATE condition is false (v43: 0 updates but 16 files rewritten).
- `numSourceRows` under-counts by 1-3 rows in a few early MERGEs; the data
  itself was verified correct (row reconciliation and duplicate check above).
- `rejected` in `batch_audit` is `incoming - valid` before the rejected-table
  dedup, which is why it is 91 higher than the rejected table.
- Silver, rejected and batch_audit are separate commits per microbatch; a
  crash between them is repaired by Spark replaying the microbatch, and every
  write is idempotent.
- The demo events copy the full Bronze row, so `UPDATE *` never relies on
  columns missing from the source.
