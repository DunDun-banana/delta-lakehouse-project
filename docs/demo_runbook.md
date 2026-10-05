# Live Demo Runbook (15 minutes)

The demo runs `notebooks/lakehouse_demo.ipynb`. It is read-only: no cell
writes, restores or vacuums a table, and versions are discovered from history.
A full run takes about 35 seconds after Spark has started.

## Day before

- [ ] `git pull`; `source .venv311/bin/activate`; `python -m pytest -q` is green.
- [ ] External SSD connected; `data/bronze`, `data/silver`, `data/gold` and
      `data/checkpoints` present (do not move or rename them).
- [ ] Silver history ends with v46 MERGE (schema), v47 OPTIMIZE, v48 OPTIMIZE ZORDER:
      `python -m scripts.show_file_stats --versions 46 48 --value 132` (no Spark, 1 s).
- [ ] Run the notebook top to bottom once; every cell finishes in under 20 s.
- [ ] No `VACUUM` has been run (it would delete the files of v43-v46).
- [ ] Keep a copy of the executed notebook / screenshots as backup (outside Git).
- [ ] Laptop on power, sleep disabled, other heavy apps closed.

## 15 minutes before

- [ ] Open JupyterLab from the repository root with `.venv311`.
- [ ] Restart the kernel and run section 0 (Spark start, about 15 s).
- [ ] Zoom the browser so tables are readable; collapse the sidebar.
- [ ] Have `docs/evidence/` and `REPORT.md` open in another tab.

## Timeline

| Time | Section | Show | Key sentence |
|---|---|---|---|
| 0:00-1:30 | Intro | README results table, architecture diagram | "48.7 M raw trips go through Bronze, Silver and Gold; every table is Delta, so every change is a version." |
| 1:30-3:30 | 1 Bronze | commit list, dirty fixture counts, v0 actions | "Bronze is append-only: one commit per file, and the dirty rows are kept on purpose so Silver can explain why they are rejected." |
| 3:30-5:30 | 2 Silver | batch_audit totals, rejection reasons | "Every rejected row keeps its raw values and all its reason codes; incoming = valid + rejected for every microbatch." |
| 5:30-9:00 | 3 CDC | discovered versions, UPDATE before/after, INSERT counts, schema columns, `versionAsOf 0` | "We do not hard-code versions: history metrics tell us which commit was the UPDATE, the INSERT and the schema change, and time travel reads the table before each one." |
| 9:00-10:30 | 4 `_delta_log` | add/remove/metaData per commit, raw JSON | "An UPDATE rewrites one file (remove + add), an INSERT only adds, and schema evolution writes a new metaData action." |
| 10:30-13:30 | 5 Z-ORDER | files per state, live Q1, benchmark table | "Compaction removes the small-file overhead; Z-ORDER makes each file cover a narrow zone range, so the JFK query reads 1 file out of 9." |
| 13:30-14:30 | 6 Gold | top 5 groups | "JFK in the afternoon is the most valuable zone-hour for drivers: about 81 dollars per trip." |
| 14:30-15:00 | Wrap-up | REPORT.md limitations | "Everything shown is reproducible from the commands in the README." |

## Fallback plan

| Problem | Action |
|---|---|
| Spark does not start (Java / port) | Restart the kernel once; if it fails again, show the saved executed notebook and say it is the output of the rehearsal run. |
| A cell is slow (> 30 s) | Skip the live Q1 timing cell and show the benchmark table from evidence. |
| SSD disconnected / data missing | Present from `docs/evidence/*.json`, `REPORT.md` and the backup notebook; never rebuild tables live. |
| `versionAsOf` fails with missing files | Someone ran VACUUM; explain retention and show the evidence JSON instead. |
| Questions on numbers | Use the tables in `REPORT.md` and the per-task docs; do not guess. |

Never run the pipeline, `optimization_benchmark.py`, `demo_silver_cdc` or
any VACUUM / OPTIMIZE / RESTORE during the demo.

## Q&A

| Question | Answer |
|---|---|
| Why is `trip_id` a hash? | TLC has no trip identifier; SHA-256 of vendor, zones, distance, pickup and dropoff gives a deterministic key that is identical on every rerun. |
| Why did the fixture not overwrite official rows? | The MERGE updates only when the row is newer **and** its `business_hash` (amount columns) differs; the fixture had the same amounts. |
| Why does v43 rewrite 16 files with 0 updates? | Delta 2.4 rewrites every file with an ON-clause match, even if the UPDATE condition is false. |
| Why is `versionAsOf 0` only 1.28 M rows? | Version 0 is the first streaming microbatch, not the full load; the load spans v0-v43. |
| What if two writers commit at once? | Optimistic concurrency: both try to create the next JSON commit; only one succeeds, the other checks for conflicts and retries or fails. |
| Why is Q3 faster on Z-ORDER? | Not verified; hypothesis: zone-clustered input helps the groupBy. Q3 reads all files in every state. |
| Why keep zero-duration trips in Gold? | They are 543,428 trips, 535,901 from VendorID 7 which never records a dropoff time; fares and distances are normal. |
| Why not partition Silver? | ~10 GB would become many small files per partition; Z-ORDER on the main filter column gives skipping without that cost. |
| Is the CDC real? | It is synthetic ingestion-time CDC appended to Bronze; TLC publishes no change feed. |
