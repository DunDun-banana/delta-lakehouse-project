"""Reproduce Delta min/max data skipping from the ``_delta_log`` JSON files.

Delta stores per-file statistics (numRecords, minValues, maxValues, nullCount
for the first 32 columns) in every ``add`` action. A query filter on a column
can skip a file when the filter range does not overlap the file's [min, max].
These helpers count how many files a query must open at a given version,
which shows the effect of OPTIMIZE / Z-ORDER independently of disk speed.

Pure Python: no Spark session is needed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def active_files(table_path: str | Path, version: int) -> dict[str, str | None]:
    """Replay JSON commits 0..version into {data file path: stats JSON or None}.

    Requires every JSON commit up to ``version`` to still exist (Delta keeps
    them for the log retention period, 30 days by default).
    """

    log_dir = Path(table_path) / "_delta_log"
    files: dict[str, str | None] = {}
    for v in range(version + 1):
        commit = log_dir / f"{v:020d}.json"
        if not commit.exists():
            raise FileNotFoundError(f"{commit} is missing; cannot replay version {version}")
        for line in commit.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                files[action["add"]["path"]] = action["add"].get("stats")
            elif "remove" in action:
                files.pop(action["remove"]["path"], None)
    return files


def column_range(stats_json: str | None, column: str) -> tuple[Any, Any] | None:
    """Return (min, max) of ``column`` from one file's stats, or None if unknown."""

    stats = json.loads(stats_json) if stats_json else {}
    low = stats.get("minValues", {}).get(column)
    high = stats.get("maxValues", {}).get(column)
    if low is None or high is None:
        return None
    return low, high


def files_to_read(
    files: dict[str, str | None],
    column: str,
    value_range: tuple[Any, Any] | None,
) -> int:
    """Count files whose [min, max] overlaps ``value_range`` (None = no filter).

    A file without statistics for ``column`` can never be skipped.
    """

    if value_range is None:
        return len(files)
    low, high = value_range
    needed = 0
    for stats_json in files.values():
        bounds = column_range(stats_json, column)
        if bounds is None or (bounds[0] <= high and bounds[1] >= low):
            needed += 1
    return needed
