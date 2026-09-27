"""Explicitly validate and enable additive Delta schema evolution."""
from pyspark.sql import DataFrame

PROTECTED = {"trip_id", "ingested_at", "raw_record_hash", "ingest_batch_id"}


def check_evolution(source: DataFrame, target: DataFrame) -> list[str]:
    existing = {f.name: f.dataType for f in target.schema.fields}
    incoming = {f.name: f.dataType for f in source.schema.fields}
    for name, dtype in existing.items():
        if name in incoming and dtype != incoming[name]:
            raise TypeError(f"Incompatible Silver column {name}: {dtype} vs {incoming[name]}")
    missing = PROTECTED - set(incoming)
    if missing:
        raise ValueError(f"Missing protected columns: {sorted(missing)}")
    return sorted(set(incoming) - set(existing))
