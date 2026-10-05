"""Pure unit tests for Task 3 history selection and Delta-log auditing."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.audit.time_travel import (
    commit_action_preview,
    commit_file_paths,
    find_column_introduction,
    find_protocol_commit,
    inspect_delta_log_commit,
    metadata_commit_versions,
    normalize_history,
    select_audit_versions,
    supports_file_scope,
)


def history_row(version: int, **metrics: int) -> dict:
    return {
        "version": version,
        "timestamp": f"2026-09-27 00:00:{version:02d}",
        "operation": "MERGE" if version else "WRITE",
        "operationParameters": {},
        "operationMetrics": {name: str(value) for name, value in metrics.items()},
    }


def schema_action(columns: list[str]) -> dict:
    return {
        "metaData": {
            "schemaString": json.dumps(
                {
                    "type": "struct",
                    "fields": [
                        {"name": column, "type": "string", "nullable": True}
                        for column in columns
                    ],
                }
            )
        }
    }


class HistorySelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = [
            history_row(0),
            history_row(
                44,
                numSourceRows=2992,
                numTargetRowsUpdated=2992,
                numTargetRowsInserted=0,
            ),
            history_row(
                45,
                numSourceRows=1,
                numTargetRowsUpdated=1,
                numTargetRowsInserted=0,
            ),
            history_row(
                46,
                numSourceRows=1,
                numTargetRowsUpdated=0,
                numTargetRowsInserted=1,
            ),
            history_row(
                47,
                numSourceRows=1,
                numTargetRowsUpdated=0,
                numTargetRowsInserted=1,
            ),
            history_row(
                48,
                numSourceRows=1,
                numTargetRowsUpdated=1,
                numTargetRowsInserted=0,
            ),
        ]

    def test_discovers_update_insert_and_avoids_schema_merge(self) -> None:
        selected = select_audit_versions(self.rows, metadata_versions={0, 48})

        self.assertEqual(selected["update_before_version"], 44)
        self.assertEqual(selected["update_after_version"], 45)
        self.assertEqual(selected["insert_before_version"], 46)
        self.assertEqual(selected["insert_after_version"], 47)

    def test_validates_explicit_version_overrides(self) -> None:
        selected = select_audit_versions(
            self.rows,
            metadata_versions={0, 48},
            update_version=48,
            insert_version=46,
        )

        self.assertEqual(selected["update_after_version"], 48)
        self.assertEqual(selected["insert_after_version"], 46)

        with self.assertRaisesRegex(ValueError, "no numTargetRowsUpdated evidence"):
            select_audit_versions(self.rows, update_version=47)

    def test_normalizes_descending_history(self) -> None:
        normalized = normalize_history(reversed(self.rows))
        self.assertEqual([row["version"] for row in normalized], [0, 44, 45, 46, 47, 48])


class DeltaLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.table_path = Path(self.temp_dir.name) / "silver"
        self.log_path = self.table_path / "_delta_log"
        self.log_path.mkdir(parents=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def write_commit(self, version: int, actions: list[dict]) -> None:
        path = self.log_path / f"{version:020d}.json"
        path.write_text(
            "\n".join(json.dumps(action) for action in actions) + "\n",
            encoding="utf-8",
        )

    def test_summarizes_commit_actions_and_schema(self) -> None:
        self.write_commit(
            0,
            [
                {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
                schema_action(["trip_id", "fare_amount"]),
                {"commitInfo": {"operation": "WRITE", "operationParameters": {}}},
                {"add": {"path": "part-000.parquet"}},
            ],
        )
        self.write_commit(
            1,
            [
                schema_action(["trip_id", "fare_amount", "surcharge_fee"]),
                {
                    "commitInfo": {
                        "operation": "MERGE",
                        "operationParameters": {"predicate": "trip_id"},
                        "operationMetrics": {"numTargetRowsUpdated": "1"},
                    }
                },
                {"remove": {"path": "part-000.parquet"}},
                {"add": {"path": "part-001.parquet"}},
                {"add": {"path": "part-002.parquet"}},
            ],
        )

        summary = inspect_delta_log_commit(self.table_path, 1, sample_files=1)

        self.assertEqual(summary["operation"], "MERGE")
        self.assertEqual(summary["add_count"], 2)
        self.assertEqual(summary["remove_count"], 1)
        self.assertEqual(summary["add_samples"], ["part-001.parquet"])
        self.assertIn("surcharge_fee", summary["metadata_columns"])
        self.assertEqual(metadata_commit_versions(self.table_path), {0, 1})

        introduction = find_column_introduction(self.table_path, "surcharge_fee")
        self.assertIsNotNone(introduction)
        self.assertEqual(introduction["version"], 1)

    def test_missing_commit_is_reported(self) -> None:
        with self.assertRaises(FileNotFoundError):
            inspect_delta_log_commit(self.table_path, 9)

    def test_invalid_json_is_reported(self) -> None:
        (self.log_path / f"{0:020d}.json").write_text("{not json}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Invalid JSON"):
            inspect_delta_log_commit(self.table_path, 0)

    def test_lists_added_and_removed_files_url_decoded(self) -> None:
        self.write_commit(
            3,
            [
                {"commitInfo": {"operation": "MERGE"}},
                {"remove": {"path": "part-00011-old%20file.parquet"}},
                {"add": {"path": "part-00011-new.parquet"}},
                {"add": {"path": "part-00004-insert.parquet"}},
            ],
        )

        files = commit_file_paths(self.table_path, 3)

        self.assertEqual(files["remove"], ["part-00011-old file.parquet"])
        self.assertEqual(
            files["add"], ["part-00011-new.parquet", "part-00004-insert.parquet"]
        )

    def test_raw_preview_has_one_truncated_line_per_action(self) -> None:
        self.write_commit(
            0,
            [
                {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
                schema_action(["trip_id"] + [f"column_{i}" for i in range(50)]),
                {"commitInfo": {"operation": "WRITE"}},
            ],
        )

        preview = commit_action_preview(self.table_path, 0, max_chars=60)

        self.assertEqual(len(preview), 3)
        self.assertTrue(preview[0].startswith('{"protocol"'))
        self.assertTrue(preview[1].endswith(" ..."))
        self.assertLessEqual(len(preview[1]), 60 + len(" ..."))

    def test_finds_latest_protocol_commit(self) -> None:
        self.write_commit(0, [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}])
        self.write_commit(1, [{"commitInfo": {"operation": "MERGE"}}])

        protocol = find_protocol_commit(self.table_path)

        self.assertEqual(protocol["version"], 0)
        self.assertEqual(protocol["protocol"]["minWriterVersion"], 2)

    def test_protocol_commit_missing_returns_none(self) -> None:
        self.write_commit(1, [{"commitInfo": {"operation": "MERGE"}}])
        self.assertIsNone(find_protocol_commit(self.table_path))


class FileScopeTests(unittest.TestCase):
    def test_plain_table_supports_file_scope(self) -> None:
        self.assertTrue(supports_file_scope({"partitionColumns": [], "minReaderVersion": 1}))

    def test_partitioned_or_column_mapped_table_falls_back(self) -> None:
        self.assertFalse(
            supports_file_scope({"partitionColumns": ["day"], "minReaderVersion": 1})
        )
        self.assertFalse(supports_file_scope({"partitionColumns": [], "minReaderVersion": 2}))


if __name__ == "__main__":
    unittest.main()
