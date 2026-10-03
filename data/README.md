# Data folder

Everything under `data/` is local-only and ignored by Git, except this file.
All paths are defined once in `src/common/config.py`.

| Path | Content | Created by |
|---|---|---|
| `source/` | `yellow_tripdata_2025-01.parquet` ... `-12.parquet` from NYC TLC (~830 MB), never modified | manual download |
| `landing/dirty_test.parquet/` | 5,218-row dirty fixture (Parquet directory) | `src.bronze.prepare_dirty_parquet` |
| `bronze/taxi_trips/` | Bronze Delta table | `src.bronze.bronze_ingestion` |
| `silver/taxi_trips/` | Silver Delta table | `src.silver.silver_pipeline` |
| `silver/rejected_records/` | rejected rows with reasons | `src.silver.silver_pipeline` |
| `silver/batch_audit/` | one row per Silver microbatch | `src.silver.silver_pipeline` |
| `checkpoints/silver_taxi/` | streaming checkpoint of the Silver query | `src.silver.silver_pipeline` |
| `gold/zone_hourly_metrics/` | Gold Delta table | `src.gold.gold_aggregation` |
| `tmp/` | Spark shuffle / spill scratch (`spark.local.dir`) | Spark |

Rules:

- Never edit files inside a Delta table directory by hand.
- `checkpoints/silver_taxi` and the three `silver/` tables belong together:
  delete or restore all four at once.
- Do not run `VACUUM` while time travel to older versions is still needed.
- `tmp/` can be emptied when no Spark job is running.
