# Task 1 - Bronze Layer Ingestion

Bronze dùng Parquet làm input và append vào Delta table. Không chuyển sang
JSON, không lọc invalid records và không deduplicate trong Bronze.

## Input

Official data:

```text
data/source/yellow_tripdata_2025-01.parquet ... yellow_tripdata_2025-12.parquet
```

Dirty fixture:

```text
data/landing/dirty_test.parquet/
```

Fixture chứa malformed dates, missing location IDs, missing synthetic
coordinates, invalid fares và duplicate records. Dữ liệu gốc trong
`data/source/` không bị sửa.

## Chạy

Từ thư mục gốc repository:

```powershell
python src\bronze\prepare_dirty_parquet.py
python src\bronze\bronze_ingestion.py
```

Lệnh ingestion mặc định đọc 12 file tháng rồi append dirty fixture vào:

```text
data/bronze/taxi_trips/
```

Mỗi file tháng là một batch. Không chạy lại cùng input nếu không muốn tạo
thêm bản ghi trùng trong append-only Bronze.

Có thể chọn batch thủ công:

```powershell
python src\bronze\bronze_ingestion.py `
  --input data\source\yellow_tripdata_2025-01.parquet `
  --input data\landing\dirty_test.parquet `
  --output data\bronze\taxi_trips
```

## Contract bàn giao cho Silver

Silver đọc Delta bằng:

```python
bronze_df = spark.read.format("delta").load("data/bronze/taxi_trips")
```

Bronze cung cấp:

- `trip_id`: giữ ID của dirty fixture hoặc tạo deterministic ID cho source.
- `tpep_pickup_datetime`, `tpep_dropoff_datetime`: lưu dạng string để giữ
  malformed dates.
- `PULocationID`, `DOLocationID`: chuẩn hóa thành long; null vẫn được giữ.
- `record_source`: `official_tlc` hoặc `generated_fixture`.
- `source_file`, `source_path`, `ingest_batch_id`, `ingested_at`,
  `raw_record_hash`: metadata lineage.

Bronze vẫn giữ `fare_amount <= 0`, missing location IDs, missing coordinates
và duplicate records. Silver chịu trách nhiệm parse dates, filter invalid
records và deduplicate theo `trip_id`.

Bronze không có CDC, `MERGE INTO`, update hoặc delete.

## Kiểm tra

```powershell
Get-ChildItem data\bronze\taxi_trips\_delta_log -Filter *.json |
  Sort-Object Name | Select-Object Name
```

Một commit được tạo cho mỗi batch append. Chạy mới toàn bộ 12 tháng và dirty
fixture sẽ tạo khoảng 13 commit trong `_delta_log/`.

## Môi trường

- PySpark 3.5.9.
- Delta Lake `delta-spark` 3.3.x.
- Java/Hadoop configuration trên Windows.
- Dữ liệu nguồn khoảng 830 MB; cần thêm dung lượng cho Delta output.

## Checklist

- [x] Đọc Parquet, không chuyển sang JSON.
- [x] Append từng monthly batch vào Delta.
- [x] Giữ malformed dates, missing IDs, missing coordinates và duplicates.
- [x] Có trip ID và metadata lineage.
- [x] Chuẩn hóa kiểu dữ liệu giữa source và dirty fixture.
- [ ] Silver đọc Bronze và thực hiện cleaning/deduplication.
