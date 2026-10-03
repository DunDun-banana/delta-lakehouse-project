# Evidence

Small JSON files produced by the real runs on the local tables. They are
committed so results can be checked without the 10 GB of data. Do not edit
them by hand; a new run writes a new file with its own date.

| File | Produced by | Content |
|---|---|---|
| `task3_audit_2026-10-03.json` | `python -m src.audit.time_travel --output docs/evidence/task3_audit_2026-10-03.json` | Task 3 audit: PASS (9/9 checks, 11.6 s), selected versions 43-46, UPDATE/INSERT/schema records, `_delta_log` summaries, protocol, latest version before/after |
| `task4_benchmark_2026-10-03.json` | `python optimization_benchmark.py --runs 5` | Task 4: environment, OPTIMIZE runs (v47 compaction, v48 Z-ORDER), per state and query: files read, 5 timings, median, speedup |

Environment of both runs: macOS laptop, 8 cores, external SSD, Python 3.11,
Spark 3.4.1, delta-spark 2.4.0.

Read them quickly with:

```bash
python -c "import json; d = json.load(open('docs/evidence/task3_audit_2026-10-03.json')); print(d['verification'], d['selected_versions'])"
python -c "import json; d = json.load(open('docs/evidence/task4_benchmark_2026-10-03.json')); [print(r['state'], r['query'], r['files_to_read'], r['median_s']) for r in d['results']]"
```

The benchmark table is also printed in section 5 of `notebooks/lakehouse_demo.ipynb`.
