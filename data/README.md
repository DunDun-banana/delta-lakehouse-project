# Data folder

## Objective

The `data/` folder is intended for sample data, small test datasets, and demonstration data used by the project.

## Rules

- Only small sample data should be committed.
- Do not commit full datasets such as a large NYC Taxi dataset.
- Do not commit generated Bronze, Silver, or Gold layer tables.
- Do not commit large Parquet or Delta table files.
- If temporary data is needed, store it under `data/sample/` in a small CSV or similar lightweight format.

## Recommended structure

- `data/sample/`: sample data for demo and local validation
- `data/bronze/`: ignored by Git and generated when the pipeline runs
- `data/silver/`: ignored by Git and generated when the pipeline runs
- `data/gold/`: ignored by Git and generated when the pipeline runs
