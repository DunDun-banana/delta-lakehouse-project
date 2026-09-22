import argparse
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq


def json_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def create_trip_id(row):
    key = "|".join(
        str(row.get(col, ""))
        for col in [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
        ]
    )
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def read_sample(parquet_path, sample_size):
    parquet = pq.ParquetFile(parquet_path)
    records = []

    for batch in parquet.iter_batches(batch_size=100_000):
        for row in batch.to_pylist():
            record = {
                key: json_value(value)
                for key, value in row.items()
            }

            record["trip_id"] = create_trip_id(record)
            records.append(record)

            if len(records) >= sample_size:
                return records

    return records


def add_dirty_values(records):
    for index, record in enumerate(records, start=1):
        pu = record.get("PULocationID")
        do = record.get("DOLocationID")

        # Tạo coordinates giả lập để test missing coordinates.
        record["pickup_latitude"] = 40.70 + ((int(pu) % 50) * 0.005) if pu else None
        record["pickup_longitude"] = -74.02 + ((int(pu) % 60) * 0.005) if pu else None
        record["dropoff_latitude"] = 40.70 + ((int(do) % 50) * 0.005) if do else None
        record["dropoff_longitude"] = -74.02 + ((int(do) % 60) * 0.005) if do else None

        # Malformed dates.
        if index % 5000 == 0:
            record["tpep_pickup_datetime"] = "not-a-date"
            record["tpep_dropoff_datetime"] = "2025-99-99 25:61:61"

        # Missing location IDs.
        if index % 3333 == 0:
            record["PULocationID"] = None

        if index % 4444 == 0:
            record["DOLocationID"] = None

        # Missing coordinates.
        if index % 2711 == 0:
            record["pickup_latitude"] = None
            record["pickup_longitude"] = None
            record["dropoff_latitude"] = None
            record["dropoff_longitude"] = None


def write_json(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, separators=(",", ":")))
            file.write("\n")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        default="data/source/yellow_tripdata_2025-01.parquet"
    )

    parser.add_argument(
        "--output-dir",
        default="data/raw_json"
    )

    parser.add_argument(
        "--sample-size",
        type=int,
        default=100_000
    )

    args = parser.parse_args()

    records = read_sample(
        Path(args.source),
        args.sample_size
    )

    add_dirty_values(records)

    batch_1 = records[:70_000]
    batch_2 = records[70_000:]

    # Thêm 250 duplicate records vào batch 2.
    batch_2.extend(batch_1[:250])

    output_dir = Path(args.output_dir)

    write_json(output_dir / "batch_001.json", batch_1)
    write_json(output_dir / "batch_002.json", batch_2)

    print("Created:", output_dir / "batch_001.json")
    print("Created:", output_dir / "batch_002.json")
    print("Batch 1 rows:", len(batch_1))
    print("Batch 2 rows:", len(batch_2))


if __name__ == "__main__":
    main()