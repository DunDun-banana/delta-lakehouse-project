# Task 3 - Time Travel and Audit Verification

## Objective

Prove, on the real Silver table and without modifying it, that every MERGE
creates a new Delta version, that the snapshot before a MERGE is still
readable with `versionAsOf`, and that history and the `_delta_log` JSON
explain exactly what changed (UPDATE, INSERT, schema evolution).

## Logic

`run_audit` in `src/audit/time_travel.py`:

1. Read `DeltaTable.history()` and normalize it (version, timestamp,
   operation, parameters, metrics).
2. Discover versions from metrics, never by hard-coding:
   UPDATE = MERGE with `numTargetRowsUpdated > 0`, INSERT = MERGE with
   `numTargetRowsInserted > 0`; prefer single-row commits (the demo pattern)
   and avoid commits with a `metaData` action for the UPDATE case.
   `--update-version` / `--insert-version` overrides are validated.
3. Find the schema commit: the first `metaData` action containing `surcharge_fee`.
4. Count rows at each before/after version with `versionAsOf`.
5. Compare before/after **file-scoped**: MERGE is copy-on-write, so the
   files removed by commit N are part of snapshot N-1 and the files added by N
   hold the new rows. Reading only those files replaces a join of two
   46 M-row snapshots. Used only when the table has no partitions and
   `minReaderVersion < 2`; otherwise a full snapshot comparison is used.
6. Summarize the commit JSON files (`commitInfo`, `add`, `remove`,
   `metaData`, `protocol`) and confirm the latest version is unchanged at the end.

## Input

`data/silver/taxi_trips` (history, data files, `_delta_log`).

## Output

Console sections (`SILVER TABLE HISTORY`, `TIME TRAVEL: BEFORE MERGE`, ...,
`TASK 3 VERIFICATION`) and an optional small JSON evidence file
(`--output`), e.g. [`docs/evidence/task3_audit_2026-10-03.json`](evidence/task3_audit_2026-10-03.json).

## Business rules

- Read-only: no RESTORE, VACUUM or write on the live Silver table.
  `restore_version` exists only for a separately copied sandbox table and is
  not reachable from the CLI.
- If no matching commit or record is found the result is `NOT VERIFIED`
  with the failing checks; no version is guessed.
- PASS requires all 9 checks: history available, historical version readable,
  update evidence, insert evidence, schema column present, schema value
  present, `_delta_log` evidence, protocol evidence, non-destructive.

## Test cases

`tests/test_time_travel.py` (pure Python, synthetic history and `_delta_log`):
version selection, override validation, history ordering, commit summary,
missing/invalid JSON, URL-decoded file paths, raw preview, protocol lookup,
file-scope eligibility.

`tests/test_time_travel_spark.py` (tiny Delta table: v0 WRITE, v1 UPDATE,
v2 INSERT, v3 schema evolution): `run_audit` returns PASS, picks v1 / v2 / v3,
row counts `{0: 3, 1: 3, 2: 4}`, latest version unchanged; `versionAsOf 0`
has no `surcharge_fee`.

## Expected result

PASS with UPDATE, INSERT and schema commits discovered automatically and the
latest Silver version identical before and after the audit.

## Actual result

Run on 2026-10-03 (`docs/evidence/task3_audit_2026-10-03.json`):

| Item | Value |
|---|---|
| Result | **PASS**, 9/9 checks, 11.6 s, method `file_scoped` |
| UPDATE | v43 -> v44, trip `93184dd0b5a98cc1efe037f60a0ab8f5f90c178bbb12809ac61b991e106c9c05`, fare 26.8 -> 28.8, tip 5.86 -> 6.86, `official_tlc` -> `cdc_demo` |
| INSERT | v44 -> v45, trip `e5f6221b7c9d9af0f4ad6a601664e9f5eaa4e6d5a1af5c89135b4913d41ac287`, rows 45,849,822 -> 45,849,823 |
| Schema | v46 adds `surcharge_fee`; demo row has `surcharge_fee` 1.5, fare 30.8, tip 7.86 |
| `_delta_log` | v44: 1 add + 1 remove; v45: 1 add, 0 remove; v46: `metaData` + 1 add + 1 remove |
| Protocol | v0 `protocol` action reader 1 / writer 2, table not partitioned, 689 files |
| Safety | latest version v46 before and after |

`spark.read.format("delta").option("versionAsOf", 0)` returns 1,276,635 rows
(the first microbatch), which shows why version 0 is not a "complete
baseline" for this table.

## Code link

- [`src/audit/time_travel.py`](../src/audit/time_travel.py)
- [`src/common/delta_utils.py`](../src/common/delta_utils.py) (`latest_commit`)

```bash
python -m src.audit.time_travel --output logs/task3_audit.json
python -m src.audit.time_travel --update-version 44 --insert-version 45
python -m scripts.peek_cdc --trip 93184dd0b5a98cc1efe037f60a0ab8f5f90c178bbb12809ac61b991e106c9c05 --before 43 --after 44
```

## Notes

- The file-scoped method made the audit about 50x faster than the earlier
  full-snapshot join (about 10 minutes).
- Time travel only works while the old data files exist: do not run `VACUUM`
  on Silver before the demo.
- The evidence was captured at v46; Silver is now at v48 after the two
  OPTIMIZE commits of Task 4, which the audit ignores (they are not MERGEs).
