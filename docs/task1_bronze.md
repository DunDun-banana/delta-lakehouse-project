# Task 2 Handoff - Silver Layer Enrichment & CDC

## 1. Trạng thái đầu vào

Các file raw cần có:

~~~
data/raw_json/batch_001.json  -> 70,000 dòng
data/raw_json/batch_002.json  -> 30,250 dòng
~~~

Sau khi chạy hai batch, Bronze Delta table nằm tại:

~~~
data/bronze/taxi_trips
~~~

Bronze dùng append-only. Batch 2 có 250 bản ghi duplicate được giữ nguyên; việc loại duplicate thuộc Silver.

Không cần commit data/bronze/, vì đây là Delta output có thể tạo lại. Nếu raw JSON không nằm trong Git, cần bàn giao artifact đó hoặc chạy lại prepare_dirty_json.py với source Parquet.

## 2. Môi trường

Cần có:

- Python virtual environment spark_env.
- PySpark 3.5.9.
- Delta Lake delta-spark 3.3.2.
- Java 8.
- Trên Windows: C:\Hadoop\bin\winutils.exe và C:\Hadoop\bin\hadoop.dll.

Kiểm tra:

~~~powershell
python --version
python -c "import pyspark, delta; print('PySpark:', pyspark.__version__); print('Delta:', delta.__file__)"
Test-Path C:\Hadoop\bin\winutils.exe
Test-Path C:\Hadoop\bin\hadoop.dll
~~~

## 3. Sửa lỗi DeltaCatalog trong Notebook

Không tạo Spark session thông thường rồi chỉ đặt config Delta. Nếu Delta JAR chưa được nạp, sẽ gặp:

~~~
Cannot find catalog plugin class for catalog 'spark_catalog':
org.apache.spark.sql.delta.catalog.DeltaCatalog
~~~

Notebook phải chạy từ thư mục gốc repository và dùng helper của Bronze:

~~~python
from pathlib import Path
import sys

PROJECT_ROOT = Path.cwd()
sys.path.insert(0, str(PROJECT_ROOT))

from src.bronze.bronze_ingestion import RAW_SCHEMA, create_spark, ingest_batch

spark = create_spark()
~~~

create_spark() đã cấu hình Delta extension, Delta catalog và Delta package. Nếu Notebook đã tạo một spark session sai cấu hình, hãy restart kernel trước khi chạy cell trên.

## 4. Chuyển JSON vào Bronze bằng PowerShell

Nếu raw JSON đã có sẵn, chạy batch 1:

~~~powershell
spark-submit.cmd --packages io.delta:delta-spark_2.12:3.3.2 src\bronze\bronze_ingestion.py --input data\raw_json\batch_001.json --output data\bronze\taxi_trips --batch-id batch_001
~~~

Sau đó chạy batch 2:

~~~powershell
spark-submit.cmd --packages io.delta:delta-spark_2.12:3.3.2 src\bronze\bronze_ingestion.py --input data\raw_json\batch_002.json --output data\bronze\taxi_trips --batch-id batch_002
~~~

Kết quả mong đợi:

~~~text
Batch 1: Appended rows: 70,000
Batch 2: Appended rows: 30,250
Bronze total rows: 100,250
~~~

Không chạy lại batch 1 nếu bảng đã tồn tại, vì lệnh dùng mode("append").

Nếu cần tạo lại raw JSON từ source Parquet:

~~~powershell
python src\bronze\prepare_dirty_json.py --source data\source\yellow_tripdata_2025-01.parquet --output-dir data\raw_json --sample-size 100000
~~~

## 5. Chuyển JSON vào Bronze trong Notebook

Dùng đúng implementation trong src/bronze/bronze_ingestion.py:

~~~python
BATCH_1 = PROJECT_ROOT / "data" / "raw_json" / "batch_001.json"
BATCH_2 = PROJECT_ROOT / "data" / "raw_json" / "batch_002.json"
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "taxi_trips"

rows_batch_1 = ingest_batch(
    spark=spark,
    input_path=BATCH_1,
    output_path=BRONZE_PATH,
    batch_id="batch_001",
)

rows_batch_2 = ingest_batch(
    spark=spark,
    input_path=BATCH_2,
    output_path=BRONZE_PATH,
    batch_id="batch_002",
)

bronze_df = spark.read.format("delta").load(str(BRONZE_PATH))

print("Batch 1 rows:", f"{rows_batch_1:,}")
print("Batch 2 rows:", f"{rows_batch_2:,}")
print("Bronze total rows:", f"{bronze_df.count():,}")
bronze_df.printSchema()
~~~

Nếu Bronze đã có hai version, chỉ đọc và kiểm tra; không chạy lại hai lệnh ingest_batch.

## 6. Kiểm tra Delta commit

~~~powershell
Get-ChildItem data\bronze\taxi_trips\_delta_log -Filter *.json | Sort-Object Name | Select-Object Name
~~~

Sau hai batch cần có:

~~~text
00000000000000000000.json
00000000000000000001.json
~~~

Commit đầu tiên phải có numOutputRows = 70000. Commit thứ hai phải có numOutputRows = 30250 và mode = Append.

## 9. Checklist nghiệm thu

- [ ] Notebook/Spark session nạp được DeltaCatalog.
- [ ] Batch 1 đọc được từ JSON và ghi Bronze.
- [ ] Batch 2 append được vào Bronze.
- [ ] Bronze có tổng 100,250 dòng sau hai batch.
- [ ] Silver loại invalid trip theo rule đã ghi.
- [ ] Silver deduplicate theo trip_id.
- [ ] MERGE có ít nhất một UPDATE và một INSERT.
- [ ] Cột surcharge_fee được thêm bằng schema evolution.
- [ ] Data cũ vẫn đọc được sau khi schema thay đổi.
- [ ] Code, test và tài liệu được commit cùng nhau.

