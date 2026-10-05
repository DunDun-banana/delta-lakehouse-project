"""Task 3 - non-destructive Delta Time Travel and audit verification.

The CLI discovers CDC versions from Delta history, compares the snapshots
immediately before/after each MERGE, and summarizes the related ``_delta_log``
JSON files. It never restores or writes the current Silver table.

MERGE is copy-on-write: files removed by commit N belong to snapshot N-1 and
files added by N hold the rewritten/inserted rows. Reading only those files
keeps the audit small instead of joining two full Silver snapshots.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.parse import unquote
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from src.common.config import SILVER_PATH
# latest_commit lives in common.delta_utils; importing it here keeps the
# Task 3 API (src.audit.time_travel.latest_commit) unchanged.
from src.common.delta_utils import latest_commit

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


COMPARE_COLUMNS = ("fare_amount", "tip_amount")


def _delta_table():
    # Lazy import keeps transaction-log unit tests runnable without Spark.
    from delta.tables import DeltaTable

    return DeltaTable


def _metric(row: Mapping[str, Any], name: str) -> int:
    try:
        return int((row.get("operationMetrics") or {}).get(name, 0) or 0)
    except (TypeError, ValueError):
        return 0


def normalize_history(rows: Iterable[Any]) -> list[dict[str, Any]]:
    """Convert Spark Rows/dicts to JSON-safe rows sorted by version."""

    result = []
    for raw in rows:
        row = raw if isinstance(raw, Mapping) else raw.asDict(recursive=True)
        if row.get("version") is None:
            continue
        result.append(
            {
                "version": int(row["version"]),
                "timestamp": str(row.get("timestamp")),
                "operation": row.get("operation"),
                "operationParameters": row.get("operationParameters") or {},
                "operationMetrics": row.get("operationMetrics") or {},
            }
        )
    return sorted(result, key=lambda row: row["version"])


def read_version(spark: "SparkSession", path: str, version: int) -> "DataFrame":
    if version < 0:
        raise ValueError("version must be >= 0")
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def history(spark: "SparkSession", path: str, limit: int = 20) -> "DataFrame":
    if limit < 1:
        raise ValueError("history limit must be >= 1")
    return _delta_table().forPath(spark, path).history(limit)


def restore_version(spark: "SparkSession", path: str, version: int):
    """Restore a separately copied sandbox table; this helper is not in the CLI."""

    if version < 0:
        raise ValueError("version must be >= 0")
    _delta_table().forPath(spark, path).restoreToVersion(version)
    return latest_commit(spark, path)


def _validated_version(
    by_version: Mapping[int, Mapping[str, Any]],
    version: int,
    metric: str,
    label: str,
) -> int:
    row = by_version.get(version)
    if row is None:
        raise ValueError(f"{label} version {version} is not in inspected history")
    if version <= 0:
        raise ValueError(f"{label} version has no readable predecessor")
    if str(row.get("operation", "")).upper() != "MERGE":
        raise ValueError(f"{label} version {version} is not a MERGE")
    if _metric(row, metric) <= 0:
        raise ValueError(f"{label} version {version} has no {metric} evidence")
    return version


def select_audit_versions(
    history_rows: Sequence[Mapping[str, Any]],
    metadata_versions: Iterable[int] = (),
    update_version: int | None = None,
    insert_version: int | None = None,
) -> dict[str, int | None]:
    """Choose small CDC MERGEs; do not assume version 0 is the baseline."""

    rows = normalize_history(history_rows)
    by_version = {row["version"]: row for row in rows}
    metadata = set(metadata_versions)

    if update_version is not None:
        update = _validated_version(
            by_version, update_version, "numTargetRowsUpdated", "UPDATE"
        )
    else:
        candidates = [
            row
            for row in rows
            if row["version"] > 0
            and str(row.get("operation", "")).upper() == "MERGE"
            and _metric(row, "numTargetRowsUpdated") > 0
        ]
        non_schema = [row for row in candidates if row["version"] not in metadata]
        candidates = non_schema or candidates
        single_row = [
            row
            for row in candidates
            if _metric(row, "numSourceRows") == 1
            and _metric(row, "numTargetRowsUpdated") == 1
            and _metric(row, "numTargetRowsInserted") == 0
        ]
        update = max((row["version"] for row in single_row or candidates), default=None)

    if insert_version is not None:
        insert = _validated_version(
            by_version, insert_version, "numTargetRowsInserted", "INSERT"
        )
    else:
        candidates = [
            row
            for row in rows
            if row["version"] > 0
            and str(row.get("operation", "")).upper() == "MERGE"
            and _metric(row, "numTargetRowsInserted") > 0
        ]
        single_row = [
            row
            for row in candidates
            if _metric(row, "numSourceRows") == 1
            and _metric(row, "numTargetRowsInserted") == 1
            and _metric(row, "numTargetRowsUpdated") == 0
        ]
        insert = max((row["version"] for row in single_row or candidates), default=None)

    return {
        "update_before_version": update - 1 if update is not None else None,
        "update_after_version": update,
        "insert_before_version": insert - 1 if insert is not None else None,
        "insert_after_version": insert,
    }


def _commit_versions(table_path: str | Path) -> list[int]:
    log_dir = Path(table_path).resolve() / "_delta_log"
    if not log_dir.is_dir():
        return []
    return sorted(
        int(path.stem)
        for path in log_dir.glob("*.json")
        if len(path.stem) == 20 and path.stem.isdigit()
    )


def read_commit_actions(table_path: str | Path, version: int) -> list[dict[str, Any]]:
    """Parse one local NDJSON commit into its Delta actions."""

    if version < 0:
        raise ValueError("version must be >= 0")
    commit = Path(table_path).resolve() / "_delta_log" / f"{version:020d}.json"
    if not commit.is_file():
        raise FileNotFoundError(f"Delta commit JSON not found: {commit}")

    actions = []
    with commit.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                actions.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {commit.name} line {line_number}: {error}"
                ) from error
    return actions


def inspect_delta_log_commit(
    table_path: str | Path, version: int, sample_files: int = 3
) -> dict[str, Any]:
    """Read one local Delta commit and retain only audit-relevant fields."""

    if version < 0 or sample_files < 0:
        raise ValueError("version and sample_files must be >= 0")

    summary = {
        "version": version,
        "file": f"{version:020d}.json",
        "operation": None,
        "operationParameters": {},
        "operationMetrics": {},
        "add_count": 0,
        "remove_count": 0,
        "add_samples": [],
        "remove_samples": [],
        "metadata_columns": None,
        "protocol": None,
    }
    for action in read_commit_actions(table_path, version):
        if "commitInfo" in action:
            info = action["commitInfo"]
            summary["operation"] = info.get("operation")
            summary["operationParameters"] = info.get("operationParameters") or {}
            summary["operationMetrics"] = info.get("operationMetrics") or {}
        for action_name in ("add", "remove"):
            if action_name in action:
                summary[f"{action_name}_count"] += 1
                samples = summary[f"{action_name}_samples"]
                if len(samples) < sample_files:
                    samples.append(action[action_name].get("path"))
        if "metaData" in action:
            schema = json.loads(action["metaData"].get("schemaString", "{}"))
            summary["metadata_columns"] = [
                field["name"] for field in schema.get("fields", [])
            ]
        if "protocol" in action:
            summary["protocol"] = action["protocol"]
    return summary


def commit_file_paths(table_path: str | Path, version: int) -> dict[str, list[str]]:
    """Return the data files added/removed by one commit (URL-decoded, relative)."""

    files: dict[str, list[str]] = {"add": [], "remove": []}
    for action in read_commit_actions(table_path, version):
        for action_name, paths in files.items():
            if action_name in action:
                paths.append(unquote(action[action_name]["path"]))
    return files


def commit_action_preview(
    table_path: str | Path, version: int, max_chars: int = 240
) -> list[str]:
    """One truncated raw JSON line per action, for showing ``_delta_log`` live."""

    if max_chars < 1:
        raise ValueError("max_chars must be >= 1")
    preview = []
    for action in read_commit_actions(table_path, version):
        text = json.dumps(action, ensure_ascii=False)
        preview.append(text if len(text) <= max_chars else text[:max_chars] + " ...")
    return preview


def find_protocol_commit(table_path: str | Path) -> dict[str, Any] | None:
    """Return the latest retained commit that carries a ``protocol`` action."""

    found = None
    for version in _commit_versions(table_path):
        summary = inspect_delta_log_commit(table_path, version, 0)
        if summary["protocol"] is not None:
            found = summary
    return found


def metadata_commit_versions(table_path: str | Path) -> set[int]:
    return {
        version
        for version in _commit_versions(table_path)
        if inspect_delta_log_commit(table_path, version, 0)["metadata_columns"] is not None
    }


def find_column_introduction(table_path: str | Path, column: str):
    """Return the retained metadata commit that first contains ``column``."""

    previous: set[str] = set()
    for version in _commit_versions(table_path):
        summary = inspect_delta_log_commit(table_path, version, 0)
        if summary["metadata_columns"] is None:
            continue
        current = set(summary["metadata_columns"])
        if column in current and column not in previous:
            return summary
        previous = current
    return None


def changed_records(
    before: "DataFrame",
    after: "DataFrame",
    key: str = "trip_id",
    columns: Sequence[str] = COMPARE_COLUMNS,
    trip_id: str | None = None,
) -> "DataFrame":
    """Find the same key with a changed fare/tip between two snapshots."""

    if key not in before.columns or key not in after.columns:
        raise ValueError(f"Both snapshots must contain {key}")
    comparable = [c for c in columns if c in before.columns and c in after.columns]
    if not comparable:
        raise ValueError("No comparison columns exist in both snapshots")

    from pyspark.sql import functions as F

    joined = before.alias("b").join(
        after.alias("a"), F.col(f"b.{key}") == F.col(f"a.{key}"), "inner"
    )
    condition = ~F.col(f"b.{comparable[0]}").eqNullSafe(F.col(f"a.{comparable[0]}"))
    for column in comparable[1:]:
        condition = condition | ~F.col(f"b.{column}").eqNullSafe(
            F.col(f"a.{column}")
        )
    if trip_id:
        joined = joined.filter(F.col(f"a.{key}") == trip_id)

    selected = [F.col(f"a.{key}").alias(key)]
    for column in comparable:
        selected += [
            F.col(f"b.{column}").alias(f"before_{column}"),
            F.col(f"a.{column}").alias(f"after_{column}"),
        ]
    for column in ("ingest_batch_id", "record_source", "ingested_at"):
        if column in before.columns and column in after.columns:
            selected += [
                F.col(f"b.{column}").alias(f"before_{column}"),
                F.col(f"a.{column}").alias(f"after_{column}"),
            ]
    return joined.filter(condition).select(*selected).limit(1)


def inserted_records(
    before: "DataFrame", after: "DataFrame", key: str = "trip_id"
) -> "DataFrame":
    """Find one key that exists after a MERGE but not before it."""

    if key not in before.columns or key not in after.columns:
        raise ValueError(f"Both snapshots must contain {key}")
    inserted = after.join(before.select(key), key, "left_anti")
    selected = [
        column
        for column in (
            key,
            "fare_amount",
            "tip_amount",
            "surcharge_fee",
            "ingest_batch_id",
            "record_source",
            "ingested_at",
        )
        if column in inserted.columns
    ]
    return inserted.select(*selected).limit(1)


def _records(frame: "DataFrame") -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in frame.collect()]


def table_detail(spark: "SparkSession", path: str) -> dict[str, Any]:
    """Table detail (same as DESCRIBE DETAIL): protocol, partitioning, file count."""

    return _delta_table().forPath(spark, path).detail().collect()[0].asDict(recursive=True)


def supports_file_scope(detail: Mapping[str, Any]) -> bool:
    """Raw Parquet rows equal Delta rows only without partitions/column mapping."""

    return not detail.get("partitionColumns") and int(detail.get("minReaderVersion") or 1) < 2


def _read_files(spark: "SparkSession", table_path: str, paths: Sequence[str]) -> "DataFrame":
    root = Path(table_path).resolve()
    return spark.read.parquet(*[str(root / path) for path in paths])


def file_scoped_update(
    spark: "SparkSession", table_path: str, version: int, trip_id: str | None = None
) -> list[dict[str, Any]]:
    """Compare rows in files removed by ``version`` (snapshot N-1) with its new files."""

    files = commit_file_paths(table_path, version)
    if not files["add"] or not files["remove"]:
        return []
    before = _read_files(spark, table_path, files["remove"])
    after = _read_files(spark, table_path, files["add"])
    return _records(changed_records(before, after, trip_id=trip_id))


def file_scoped_insert(
    spark: "SparkSession", table_path: str, version: int, key: str = "trip_id"
) -> list[dict[str, Any]]:
    """Take a key only in files added by ``version`` and prove it is absent at N-1."""

    from pyspark.sql import functions as F

    files = commit_file_paths(table_path, version)
    if not files["add"]:
        return []
    after = _read_files(spark, table_path, files["add"])
    before = (
        _read_files(spark, table_path, files["remove"])
        if files["remove"]
        else after.limit(0)
    )
    rows = _records(inserted_records(before, after, key))
    if not rows:
        return []
    # Filter-only scan of the key column at N-1 (no shuffle, no full join).
    previous = read_version(spark, table_path, version - 1)
    if previous.filter(F.col(key) == rows[0][key]).select(key).limit(1).collect():
        return []
    return rows


def schema_value_record(
    spark: "SparkSession",
    table_path: str,
    schema_commit: Mapping[str, Any] | None,
    column: str,
    key: str = "trip_id",
) -> dict[str, Any] | None:
    """Return one row written by the schema-evolution commit with ``column`` set."""

    from pyspark.sql import functions as F

    if not schema_commit:
        return None
    added = commit_file_paths(table_path, schema_commit["version"])["add"]
    if not added:
        return None
    frame = _read_files(spark, table_path, added)
    if column not in frame.columns:
        return None
    selected = [
        c
        for c in (key, column, "fare_amount", "tip_amount", "ingest_batch_id", "record_source")
        if c in frame.columns
    ]
    rows = _records(frame.filter(F.col(column).isNotNull()).select(*selected).limit(1))
    return rows[0] if rows else None


def _section(title: str, value: Any | None = None) -> None:
    print(f"\n=== {title} ===")
    if value is not None:
        print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def run_audit(
    spark: "SparkSession",
    silver_path: str,
    history_limit: int = 100,
    update_version: int | None = None,
    insert_version: int | None = None,
    trip_id: str | None = None,
    schema_column: str = "surcharge_fee",
    output: str | Path | None = None,
) -> dict[str, Any]:
    """Execute Task 3 without mutating Silver and return reusable evidence."""

    from pyspark.sql import functions as F

    started = time.perf_counter()
    DeltaTable = _delta_table()
    if not DeltaTable.isDeltaTable(spark, silver_path):
        raise FileNotFoundError(f"Silver Delta table not found: {silver_path}")
    latest_before = latest_commit(spark, silver_path)
    if latest_before is None:
        raise RuntimeError("Silver Delta history is empty")

    rows = normalize_history(history(spark, silver_path, history_limit).collect())
    versions = select_audit_versions(
        rows,
        metadata_commit_versions(silver_path),
        update_version,
        insert_version,
    )
    detail = table_detail(spark, silver_path)
    file_scoped = supports_file_scope(detail)
    method = "file_scoped" if file_scoped else "snapshot_join"

    _section("SILVER TABLE HISTORY")
    for row in reversed(rows):
        print(
            f"v{row['version']} | {row['timestamp']} | {row['operation']} | "
            f"parameters={json.dumps(row['operationParameters'], default=str)} | "
            f"metrics={json.dumps(row['operationMetrics'], default=str)}"
        )
    _section("TIME TRAVEL: BEFORE MERGE", versions)
    _section("CURRENT SILVER VERSION", latest_before)

    snapshot_counts: dict[int, int] = {}
    _section("TIME TRAVEL: SNAPSHOT ROW COUNTS")
    for version in sorted({v for v in versions.values() if v is not None}):
        snapshot_counts[version] = read_version(spark, silver_path, version).count()
        print(f"versionAsOf {version}: {snapshot_counts[version]:,} rows")

    update_rows: list[dict[str, Any]] = []
    _section(f"CDC UPDATE AUDIT ({method})")
    if versions["update_after_version"] is not None:
        if file_scoped:
            update_rows = file_scoped_update(
                spark, silver_path, versions["update_after_version"], trip_id
            )
        else:
            before = read_version(spark, silver_path, versions["update_before_version"])
            after = read_version(spark, silver_path, versions["update_after_version"])
            update_rows = _records(changed_records(before, after, trip_id=trip_id))
    print(
        json.dumps(update_rows[0], indent=2, default=str)
        if update_rows
        else "NOT VERIFIED: no changed fare/tip record found"
    )

    insert_rows: list[dict[str, Any]] = []
    _section(f"CDC INSERT AUDIT ({method})")
    if versions["insert_after_version"] is not None:
        if file_scoped:
            insert_rows = file_scoped_insert(
                spark, silver_path, versions["insert_after_version"]
            )
        else:
            before = read_version(spark, silver_path, versions["insert_before_version"])
            after = read_version(spark, silver_path, versions["insert_after_version"])
            insert_rows = _records(inserted_records(before, after))
    print(
        json.dumps(insert_rows[0], indent=2, default=str)
        if insert_rows
        else "NOT VERIFIED: no newly inserted trip found"
    )

    current = spark.read.format("delta").load(silver_path)
    current_columns = current.schema.fieldNames()
    schema_commit = find_column_introduction(silver_path, schema_column)
    if file_scoped:
        schema_record = schema_value_record(spark, silver_path, schema_commit, schema_column)
    elif schema_column in current_columns:
        found = _records(current.filter(F.col(schema_column).isNotNull()).limit(1))
        schema_record = found[0] if found else None
    else:
        schema_record = None
    _section("SCHEMA EVOLUTION VALUE")
    print(
        json.dumps(schema_record, indent=2, default=str)
        if schema_record
        else f"NOT VERIFIED: no non-null {schema_column} value found"
    )

    protocol_commit = find_protocol_commit(silver_path)
    protocol = {
        "log_version": protocol_commit["version"] if protocol_commit else None,
        "log_action": protocol_commit["protocol"] if protocol_commit else None,
        **{
            name: detail.get(name)
            for name in (
                "minReaderVersion",
                "minWriterVersion",
                "partitionColumns",
                "numFiles",
                "sizeInBytes",
            )
        },
    }
    _section("PROTOCOL AND TABLE DETAIL", protocol)

    commit_versions = {
        version
        for version in (
            versions["update_after_version"],
            versions["insert_after_version"],
            schema_commit["version"] if schema_commit else None,
        )
        if version is not None
    }
    log_summaries = []
    _section("DELTA TRANSACTION LOG")
    for version in sorted(commit_versions):
        try:
            summary = inspect_delta_log_commit(silver_path, version)
            summary["raw_preview"] = commit_action_preview(silver_path, version)
        except FileNotFoundError as error:
            print(f"NOT VERIFIED: {error}")
            continue
        log_summaries.append(summary)
        print(json.dumps(summary, indent=2, default=str))

    latest_after = latest_commit(spark, silver_path)
    checks = {
        "history_available": len(rows) >= 2,
        "historical_version_readable": any(
            versions[name] in snapshot_counts
            for name in ("update_before_version", "insert_before_version")
        ),
        "update_evidence": bool(update_rows),
        "insert_evidence": bool(insert_rows),
        "schema_column_present": schema_column in current_columns,
        "schema_value_present": schema_record is not None,
        "delta_log_evidence": bool(log_summaries),
        "protocol_evidence": protocol_commit is not None,
        "non_destructive": bool(
            latest_after and latest_before["version"] == latest_after["version"]
        ),
    }
    verification = "PASS" if all(checks.values()) else "NOT VERIFIED"
    evidence = {
        "silver_path": str(Path(silver_path).resolve()),
        "method": method,
        "latest_before": latest_before,
        "latest_after": latest_after,
        "selected_versions": versions,
        "snapshot_counts": snapshot_counts,
        "history": rows,
        "update_record": update_rows[0] if update_rows else None,
        "insert_record": insert_rows[0] if insert_rows else None,
        "schema_column": schema_column,
        "schema_introduction": schema_commit,
        "schema_value_record": schema_record,
        "protocol": protocol,
        "delta_log": log_summaries,
        "checks": checks,
        "verification": verification,
        "runtime_seconds": round(time.perf_counter() - started, 1),
    }
    _section(
        "TASK 3 VERIFICATION",
        {"result": verification, **checks, "runtime_seconds": evidence["runtime_seconds"]},
    )

    if output:
        output_path = Path(output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"Evidence JSON: {output_path}")
    return evidence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--silver", default=str(SILVER_PATH))
    parser.add_argument("--history", dest="history_limit", type=int, default=100)
    parser.add_argument("--update-version", type=int)
    parser.add_argument("--insert-version", type=int)
    parser.add_argument("--trip-id")
    parser.add_argument("--schema-column", default="surcharge_fee")
    parser.add_argument("--output", help="Optional small JSON evidence file")
    parser.add_argument("--master", default="local[2]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    from src.common.spark import create_spark

    args = parse_args(argv)
    spark = create_spark("time-travel-audit", args.master)
    try:
        evidence = run_audit(
            spark,
            args.silver,
            args.history_limit,
            args.update_version,
            args.insert_version,
            args.trip_id,
            args.schema_column,
            args.output,
        )
        if evidence["verification"] != "PASS":
            raise SystemExit(2)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
