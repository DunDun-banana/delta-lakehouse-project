# Delta Lakehouse Architecture & Storage Optimization

## Project objective

This project builds a medallion lakehouse architecture:

Bronze -> Silver -> Gold

The goal is to help the team practice:

- ingesting raw data into the bronze layer
- cleaning, standardizing, and merging CDC records into the silver layer
- creating a gold layer for analytics and reporting
- validating reliability and optimizing storage and performance with Delta Lake

## Project structure

- `src/bronze/`: ingestion and raw-to-bronze processing
- `src/silver/`: cleaning, deduplication, merge, and schema evolution
- `src/gold/`: aggregation and business-ready tables
- `src/audit/`: time travel, audit checks, and validation
- `src/optimization/`: optimize, Z-ORDER, and benchmarks
- `docs/`: task documentation
- `tests/`: unit test skeletons
- `notebooks/`: demo notebooks
- `data/source/`: local official monthly NYC TLC Parquet inputs
- `data/landing/`: local generated dirty Parquet fixture for Bronze
- `data/sample/`: small sample data

## Environment

### Requirements

- Python 3.10+
- Java runtime required for PySpark
- Local machine or dev cluster with enough memory for demo execution

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How to run the project

```bash
python lakehouse_pipeline.py
python optimization_benchmark.py
```

If needed, open each module in its corresponding folder and run it according to the task-specific instructions.

## Team working conventions

- Each code task must have a corresponding documentation file in `docs/`.
- Documents must explain the logic, input/output, test cases, expected result, actual result, and notes.
- Do not push directly to `main`.
- Use a separate feature branch for each task.
- Each Pull Request must include code, documentation, and test skeletons when applicable.
- Do not commit large datasets, large Delta tables, or oversized Parquet files.

## Main workflow

1. Bronze Layer
   - ingest raw data
   - store it in a Delta table
2. Silver Layer
   - clean records, filter invalid rows, merge CDC updates
3. Gold Layer
   - create aggregates for reporting and dashboards
4. Audit & Optimization
   - time travel, VACUUM, OPTIMIZE, Z-ORDER, and benchmarking

## Notes

- Bronze ingestion is implemented for monthly Parquet batches and the dirty
  Parquet fixture.
- Silver, Gold, audit, optimization, and the end-to-end orchestration remain
  follow-up work and should be completed with their related documentation.
