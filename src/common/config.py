"""Project paths shared by every layer, script, test and notebook.

All paths are absolute and derived from the repository root, so commands work
from any working directory. Changing a path here moves the table for every
module at once; the existing Delta tables and the Silver streaming checkpoint
live at exactly these locations.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Inputs
SOURCE_DIR = ROOT / "data" / "source"
DIRTY_FIXTURE = ROOT / "data" / "landing" / "dirty_test.parquet"
DIRTY_FIXTURE_SOURCE = SOURCE_DIR / "yellow_tripdata_2025-11.parquet"

# Delta tables
BRONZE_PATH = ROOT / "data" / "bronze" / "taxi_trips"
SILVER_PATH = ROOT / "data" / "silver" / "taxi_trips"
REJECTED_PATH = ROOT / "data" / "silver" / "rejected_records"
BATCH_AUDIT_PATH = ROOT / "data" / "silver" / "batch_audit"
GOLD_PATH = ROOT / "data" / "gold" / "zone_hourly_metrics"

# Runtime state. The checkpoint belongs to the Silver tables above: delete it
# only together with Silver, rejected_records and batch_audit.
SILVER_CHECKPOINT = ROOT / "data" / "checkpoints" / "silver_taxi"
SPARK_TMP_DIR = ROOT / "data" / "tmp"

# Small JSON evidence files that are committed to Git.
EVIDENCE_DIR = ROOT / "docs" / "evidence"
