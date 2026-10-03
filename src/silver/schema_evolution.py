"""Guard rails for additive Delta schema evolution in the Silver MERGE."""

from __future__ import annotations

from pyspark.sql import DataFrame

# Columns the MERGE condition and lineage depend on; a batch without them is
# rejected instead of silently writing NULL keys or hashes.
PROTECTED = {"trip_id", "ingested_at", "raw_record_hash", "ingest_batch_id", "business_hash"}


def check_evolution(source: DataFrame, target: DataFrame) -> list[str]:
    """Allow only new columns; return them sorted.

    Raises TypeError when an existing column changes type and ValueError when
    a protected column is missing from the source.
    """

    existing = {f.name: f.dataType for f in target.schema.fields}
    incoming = {f.name: f.dataType for f in source.schema.fields}
    for name, dtype in existing.items():
        if name in incoming and dtype != incoming[name]:
            raise TypeError(f"Incompatible Silver column {name}: {dtype} vs {incoming[name]}")
    missing = PROTECTED - set(incoming)
    if missing:
        raise ValueError(f"Missing protected columns: {sorted(missing)}")
    return sorted(set(incoming) - set(existing))
