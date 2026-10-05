"""Export Silver Delta history and the per-microbatch audit to a small JSON file.

    python -m scripts.export_silver_history --out logs/silver_version_history.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from delta.tables import DeltaTable

from src.audit.time_travel import history
from src.common.config import BATCH_AUDIT_PATH, ROOT, SILVER_PATH
from src.common.spark import create_spark


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", default=str(SILVER_PATH))
    parser.add_argument("--audit", default=str(BATCH_AUDIT_PATH))
    parser.add_argument("--out", default=str(ROOT / "logs" / "silver_version_history.json"))
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)

    spark = create_spark("export-silver-history", "local[2]")
    try:
        result = {
            "silver_path": args.silver,
            "history": [
                row.asDict(recursive=True)
                for row in history(spark, args.silver, args.limit).collect()
            ],
        }
        if DeltaTable.isDeltaTable(spark, args.audit):
            audit = spark.read.format("delta").load(args.audit).orderBy("batch_id")
            result["batch_audit"] = [
                row.asDict(recursive=True) for row in audit.limit(args.limit).collect()
            ]
        output = Path(args.out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, default=str, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
