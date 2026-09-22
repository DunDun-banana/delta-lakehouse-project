# Data folder

## Objective

The `data/` folder is intended for sample data, small test datasets, and demonstration data used by the project.

## Rules

- Only small sample data should be committed.
- Do not commit full datasets such as a large NYC Taxi dataset.
- Do not commit generated Bronze, Silver, or Gold layer tables.
- Do not commit large Parquet or Delta table files.
- Generated landing fixtures are local-only and ignored by Git.
- If a tiny tracked fixture is needed for unit tests, store it under `data/sample/`.

## Recommended structure

- `data/source/`: downloaded official NYC TLC monthly Parquet files; local-only
- `data/landing/`: generated `dirty_test.parquet` used by Bronze ingestion; local-only
- `data/sample/`: tiny tracked data for demo and local validation
- `data/bronze/`: ignored by Git and generated when the pipeline runs
- `data/silver/`: ignored by Git and generated when the pipeline runs
- `data/gold/`: ignored by Git and generated when the pipeline runs
