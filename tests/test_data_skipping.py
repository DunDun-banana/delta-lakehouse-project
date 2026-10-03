"""Data skipping replay on a synthetic _delta_log (no Spark)."""

from __future__ import annotations

import json

import pytest

from src.optimization.data_skipping import active_files, column_range, files_to_read

COLUMN = "PULocationID"


def add(path, low=None, high=None):
    stats = None
    if low is not None:
        stats = json.dumps({"numRecords": 10, "minValues": {COLUMN: low}, "maxValues": {COLUMN: high}})
    return {"add": {"path": path, "stats": stats}}


def remove(path):
    return {"remove": {"path": path}}


@pytest.fixture
def table(tmp_path):
    log = tmp_path / "_delta_log"
    log.mkdir()
    commits = [
        # v0: baseline - every file spans almost all zones (unordered data)
        [add("a", 1, 265), add("b", 2, 264), add("c", 1, 263)],
        # v1: OPTIMIZE ZORDER BY - same data rewritten into narrow zone ranges
        [remove("a"), remove("b"), remove("c"), add("z1", 1, 100), add("z2", 100, 180), add("z3", 181, 265)],
    ]
    for version, actions in enumerate(commits):
        (log / f"{version:020d}.json").write_text("\n".join(json.dumps(a) for a in actions) + "\n")
    return tmp_path


def test_replay_applies_add_and_remove(table):
    assert set(active_files(table, 0)) == {"a", "b", "c"}
    assert set(active_files(table, 1)) == {"z1", "z2", "z3"}


def test_zorder_reduces_files_to_read(table):
    before, after = active_files(table, 0), active_files(table, 1)
    assert files_to_read(before, COLUMN, (132, 132)) == 3
    assert files_to_read(after, COLUMN, (132, 132)) == 1
    assert files_to_read(after, COLUMN, (90, 120)) == 2
    assert files_to_read(after, COLUMN, None) == 3


def test_file_without_stats_is_always_read():
    files = {"with_stats": add("x", 1, 10)["add"]["stats"], "no_stats": None}
    assert column_range(None, COLUMN) is None
    assert files_to_read(files, COLUMN, (200, 210)) == 1


def test_missing_commit_is_reported(table):
    with pytest.raises(FileNotFoundError):
        active_files(table, 5)
