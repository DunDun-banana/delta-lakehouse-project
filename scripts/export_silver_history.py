"""Export Silver Delta history and per-batch audit to JSON (small metadata only)."""
import argparse
import json
from pathlib import Path
from src.silver.silver_pipeline import ROOT, make_spark
from src.audit.time_travel import history
from delta.tables import DeltaTable


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--silver", default=str(ROOT / "data/silver/taxi_trips"))
    p.add_argument("--audit", default=str(ROOT / "data/silver/batch_audit"))
    p.add_argument("--out", default=str(ROOT / "logs/silver_version_history.json"))
    p.add_argument("--limit", type=int, default=100)
    args = p.parse_args()
    spark = make_spark("local[2]")
    try:
        result = {"silver_path": args.silver, "history": [x.asDict(recursive=True) for x in history(spark, args.silver, args.limit).collect()]}
        if DeltaTable.isDeltaTable(spark, args.audit):
            result["batch_audit"] = [x.asDict(recursive=True) for x in spark.read.format("delta").load(args.audit).orderBy("batch_id").limit(args.limit).collect()]
        output = Path(args.out); output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2), encoding="utf8")
        print(output)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
