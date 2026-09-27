# Task 2 - Silver Layer & CDC

## Objective

TODO: Describe the goal of this task.

## Logic / implementation approach

TODO:

- define the cleaning rules for the Bronze data
- decide how duplicates are identified and removed
- define the CDC merge process for inserts and updates
- specify how the Silver Delta table is written

## Input

TODO:

- Bronze Delta table name
- source dataset or CDC feed
- expected key columns and update logic

## Output

TODO:

- Silver Delta table location
- cleaned dataset summary
- merge results and validation notes

## Business rules

TODO:

- define valid vs invalid records
- define how duplicates are resolved
- define update behavior for existing rows
- document schema evolution assumptions

## Test cases

TODO:

- test clean records pass through
- test duplicates are removed
- test update merge works correctly
- test insert-only scenarios
- test invalid rows are excluded or corrected

## Expected result

TODO: Describe the expected Silver layer behavior after the task is complete.

--- 

## Actual result

TBD

# TASK 2 — SILVER LAYER & CDC (HƯỚNG DẪN TRIỂN KHAI VÀ NGHIỆM THU)

> Phạm vi: phần Silver của `delta-lakehouse-project`; Windows PowerShell, chạy từ **thư mục gốc repository**. Tài liệu này thay thế bản Task 2 còn TODO. Sau khi hoàn thành Task 2 mới chuyển sang [`task3_time_travel.md`](task3_time_travel.md). Các số liệu và version trong ví dụ là minh họa, không phải kết quả đã đo trên máy nhóm.

## 1. Mục tiêu (Objective)

Đọc bảng Bronze **Delta** chứa dữ liệu NYC Yellow Taxi và dirty fixture, chuẩn hóa thời gian và kiểu dữ liệu, lọc bản ghi không hợp lệ, loại bản ghi trùng theo `trip_id`, ghi bảng Silver **Delta**, rồi xử lý CDC bằng `MERGE INTO`: bản ghi `trip_id` đã tồn tại được UPDATE khi dữ liệu đến sau theo chính sách hiện tại; `trip_id` mới được INSERT. Chứng minh việc thêm cột `surcharge_fee` qua schema evolution. Dữ liệu bị từ chối cần được lưu riêng để giải thích nguyên nhân, và mỗi microbatch thành công có audit.

## 2. Input

| Nguồn | Đường dẫn | Ý nghĩa |
|---|---|---|
| Parquet gốc | `data/source/yellow_tripdata_*.parquet` | 12 tháng đang có trên máy; số file thực tế phụ thuộc thư mục source |
| Fixture bẩn | `data/landing/dirty_test.parquet` | Tạo bằng script của Bronze, chứa ngày sai, giá không hợp lệ, location/GPS thiếu, duplicate |
| Bronze Delta | `data/bronze/taxi_trips` | **Đầu vào trực tiếp** của Silver, không phải đọc lại 12 file nguồn |
| Metadata Bronze | `trip_id`, `ingested_at`, `ingest_batch_id`, `raw_record_hash`, `record_source` | ID mô phỏng, thứ tự ingest, chống lặp, nhận biết fixture |
| Các cột nghiệp vụ | `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, ... | Kiểm tra chất lượng/CDC |

Bronze giữ ngày ở dạng string để Silver phát hiện ngày lỗi. `trip_id` của file TLC chính thức là **synthetic SHA-256 từ một nhóm trường nghiệp vụ**, không phải mã chuyến đi chính thức; hai chuyến có cùng đầu vào hash có thể bị gộp nhầm. `ingested_at` là thời điểm ingest, **không phải event time thực từ nguồn**. Dirty fixture có GPS tổng hợp; file TLC chính thức có zone IDs và không được reject chỉ vì thiếu GPS.

### Bronze: chỉ chuẩn bị/chạy khi chưa có bảng

Nếu `data/bronze/taxi_trips/_delta_log` đã tồn tại từ lần ingest đủ 12 tháng + fixture thì **bỏ qua hai lệnh sau**; code Bronze hiện append-only và chạy lại sẽ append trùng:

```powershell
python src/bronze/prepare_dirty_parquet.py --source data/source/yellow_tripdata_2025-11.parquet
python src/bronze/bronze_ingestion.py
```

Lệnh không truyền `--input` nạp các `yellow_tripdata_*.parquet` sắp xếp theo tên rồi dirty fixture. Nếu máy đã đổi bộ tháng, thay tên `--source` bằng file đang có. Không xóa Bronze/source để sửa lỗi Silver.

## 4. Logic triển khai (Implementation approach)

```text
Bronze Delta + checkpoint
    -> readStream Delta (soft limit maxBytesPerTrigger)
    -> foreachBatch
    -> validate_and_split (valid / rejected + lý do)
    -> deduplicate(valid) theo trip_id, ingested_at DESC, ingest_batch_id DESC, hash DESC
    -> Delta MERGE Silver (UPDATE khi nguồn mới hơn; INSERT nếu trip_id chưa có)
    -> MERGE rejected_records (khóa ingest_batch_id + raw_record_hash)
    -> upsert batch_audit (batch_id và version Silver trước/sau)
    -> foreachBatch thành công -> Spark ghi checkpoint/commits
```

**Không nhầm lẫn:** một Bronze Delta commit có thể được Spark chia/gộp thành nhiều microbatch; một lượt `--once` có thể tạo **nhiều** version Silver. Không được mặc định `Silver v0` là toàn bộ 12 tháng.

## 5. Business rules (quy tắc dữ liệu)

| Nhóm | Quy tắc hiện có trong `silver_cleaning.py` |
|---|---|
| Khóa CDC | `trip_id` không null/rỗng |
| Ngày | Parse pickup/dropoff theo `yyyy-MM-dd HH:mm:ss`, không parse được hoặc dropoff trước pickup → reject |
| Địa điểm | `PULocationID`, `DOLocationID` không null và `> 0` |
| Fare | `fare_amount` không null/NaN và `> 0` |
| Khác | `trip_distance >= 0`, `total_amount > 0`; passenger_count nếu có không được âm |
| GPS | **Chỉ** yêu cầu/kiểm tra các tọa độ tổng hợp của `record_source = generated_fixture`; không loại tất cả official TLC vì GPS null |
| Dedupe trong batch | Chọn 1 bản ghi/`trip_id` theo `ingested_at`, `ingest_batch_id`, `raw_record_hash` giảm dần |
| MERGE qua các batch | Trip tồn tại chỉ thay khi nguồn được coi mới hơn; trip mới INSERT |
| Rejected | Giữ `rejection_reasons`, `raw_pickup_datetime`, `raw_dropoff_datetime` để audit dữ liệu bẩn |
| Schema evolution | Cho phép thêm cột additive, ví dụ `surcharge_fee: double`; không mặc định chấp nhận đổi kiểu cột cũ/xóa `trip_id` |

Chú ý: nếu batch mới không có giá trị cho cột cũ, `whenMatchedUpdateAll()` có thể ghi null; bài hiện tại **chưa** phải engine CDC production có quy tắc partial update/source sequence. `record_source=cdc_demo` chỉ là CDC tổng hợp cho live demo.

## 6. Output

| Đường dẫn | Nội dung cần có |
|---|---|
| `data/silver/taxi_trips/` | Delta table sạch, duy nhất theo `trip_id` trong logic mô phỏng |
| `data/silver/rejected_records/` | Delta table records bị loại kèm lý do, nếu thực sự có rejected |
| `data/silver/batch_audit/` | Delta table `batch_id`, incoming, valid, rejected, winners, Silver version trước/sau... |
| `data/checkpoints/silver_taxi/commits/` | Chứng cứ microbatch đã hoàn thành; chỉ xuất hiện sau khi `foreachBatch` kết thúc thành công |
| `data/silver/taxi_trips/_delta_log/` | Log transaction **thật** của Delta; không đồng nghĩa với application log |

Silver, rejected và audit là **ba Delta transaction độc lập**, không phải một ACID transaction xuyên ba bảng. Khi lỗi giữa chừng, Spark sẽ thử chạy lại microbatch chưa commit; không xóa riêng checkpoint.

## 7. Hướng dẫn chạy Task 2 theo đúng thứ tự (PowerShell)

- sau mỗi lần chạy xong có thể tổng hợp các version thành json hoặc xem thông tin latest version 


```powershell
python -m scripts.export_silver_history
python -m src.audit.time_travel --history 1
```

Nếu chỉ muốn chạy 1 file thử demo thì 

```bash
python src/bronze/bronze_ingestion.py --input data/source/yellow_tripdata_2025-12.parquet
```

### 7.1. Chạy Silver lần đầu

```powershell
python -m src.silver.silver_pipeline --once --max-bytes-per-trigger 128m
```

Chờ process kết thúc không báo exception. Có thể đổi `128m` thành `64m` khi máy thiếu RAM/ổ tmp; đây là **soft limit** theo file Delta. Không chạy script CDC cho đến khi Silver lần đầu hoàn thành. Kiểm tra:

```powershell
Get-ChildItem data/silver/taxi_trips/_delta_log -Filter *.json | Sort-Object Name | Select-Object -Last 5
Get-ChildItem data/checkpoints/silver_taxi/commits | Select-Object -Last 5
Get-ChildItem data/silver/batch_audit/_delta_log -Filter *.json | Select-Object -Last 5
```

Nếu `batch_audit` hoặc `commits` chưa có, **xem stack trace lỗi trước**; trước đây code lỗi `.withSchemaEvolution()` tại MERGE và checkpoint không được commit. Nếu Silver từng hoàn thành với quy tắc GPS cũ, sửa code rồi chạy tiếp cùng checkpoint **không tự làm sạch lại batch đã commit**. Với demo sạch, dừng mọi Spark job, backup đồng bộ `data/silver/{taxi_trips,rejected_records,batch_audit}` và `data/checkpoints/silver_taxi` rồi mới xây Silver mới từ Bronze còn nguyên. Không reset nếu muốn giữ bằng chứng history trước đó.
 
### 7.2. CDC UPDATE (không thay schema)

```powershell
python -m scripts.demo_silver_cdc --mode update
python -m src.silver.silver_pipeline --once --max-bytes-per-trigger 256m
```

Lưu `trip_id` và `ingest_batch_id` script in ra; dùng các giá trị đó để đối chiếu *cùng một trip* ở Silver trước/sau MERGE. Giá fare/tip phải thay đổi; số lượng bản ghi của trip đó phải là 1.

### 7.3. CDC INSERT

```powershell
python -m scripts.demo_silver_cdc --mode insert
python -m src.silver.silver_pipeline --once --max-bytes-per-trigger 256m
```

Script tạo **trip_id mới** trong batch demo. Trước khi Silver xử lý, trip mới chưa tồn tại trong Silver; sau xử lý, phải có đúng 1 bản ghi.

### 7.4. Schema evolution, thêm `surcharge_fee`

```powershell
python -m scripts.demo_silver_cdc --mode schema
python -m src.silver.silver_pipeline --once --max-bytes-per-trigger 128m
```

Đoạn demo append thêm `surcharge_fee` vào **Bronze Delta** rồi Silver MERGE. Kiểm tra cột mới đã có trong Silver **và có ít nhất một giá trị 1.5 ở bản ghi demo**. Các dòng lịch sử không có cột sẽ đọc null ở snapshot schema mới; snapshot trước khi thêm cột không có cột đó. Cột mới khác như `payment_processor` chỉ được chứng minh sau khi có batch và test tương ứng; đừng tuyên bố đã demo mọi feature.

**Rủi ro:** `autoMerge` cho *bảng Silver đích* không bảo đảm streaming *nguồn Bronze* nhận thay đổi schema liền mạch. Nếu gặp schema-change exception, giữ nguyên checkpoint, lưu lỗi, restart process một lần; nếu vẫn lỗi cần xử lý riêng, không xóa checkpoint rồi tuyên bố incremental thành công.

### 7.5. Thống kê/tái hiện kết quả cho Task 2

```powershell
python -m pytest -q tests/test_task2_task3.py
python -m scripts.export_silver_history
```

Trong notebook `notebooks/02_silver_demo.ipynb`: chạy nhóm cell inspect Bronze; demo dirty fixture, rejection reasons; kiểm tra official TLC hợp lệ; inspect Silver/rejected/audit; ghi lại fare/tip **của chính trip_id** trước & sau update; hiển thị trip mới và schema `surcharge_fee`. Không mở notebook và chạy pipeline đồng thời nếu RAM yếu. Cần ghi version từ Delta history, không đoán `v0/v1`.

## 8. Test cases cần nộp

| Case | Thao tác | Kỳ vọng |
|---|---|---|
| T2-01 | Bản ghi fare > 0, location hợp lệ | Vào Silver nếu không vi phạm rule khác |
| T2-02 | Fare <= 0 hoặc null/NaN | Rejected với `invalid_fare` |
| T2-03 | Thiếu PU/DO zone | Rejected với `invalid_location` |
| T2-04 | Ngày sai / dropoff < pickup | Rejected, giữ ngày raw |
| T2-05 | Duplicate cùng `trip_id` trong microbatch | Chỉ giữ winner |
| T2-06 | UPDATE fare/tip cùng trip ID | Cập nhật bản ghi cũ; không tạo trip trùng |
| T2-07 | INSERT trip ID mới | Silver thêm trip mới |
| T2-08 | Thêm `surcharge_fee` | Silver có cột và giá trị mới, không lỗi MERGE đích |
| T2-09 | Re-run `--once` khi Bronze không thêm commit | Không xử lý lại các batch đã checkpoint hoàn thành |
| T2-10 | Official TLC thiếu GPS nhưng zone hợp lệ | Không bị reject **chỉ vì GPS** |

`tests/test_task2_task3.py` hiện kiểm tra một số case cốt lõi với dữ liệu nhỏ; **không khẳng định toàn bộ T2-01…T2-10 đều đã có automated test**. Thực hiện thêm kiểm tra trực tiếp/ghi ảnh Spark UI khi nộp.

## 9. Expected result (kỳ vọng)

- Đủ dữ liệu TLC hợp lệ vào Silver, dirty fixture có rejected reasons; Silver không tăng vô cớ vì duplicate cùng `trip_id`.
- Có commit Silver ban đầu, MERGE UPDATE và MERGE INSERT; `surcharge_fee` được thêm trên batch schema.
- Có audit và checkpoint cho các microbatch thành công; số lượng version thực tế theo commit, **không cố định 0–3**.
- Có `trip_id`, fare/tip trước/sau và giá trị surcharge thực để sang Task 3 chứng minh Time Travel.

## 10. Actual result (điền SAU khi chạy trên máy nhóm)

| Mục | Số liệu/ảnh minh chứng thực tế |
|---|---|
| Phiên bản PySpark/Delta/Java | `...` |
| Số file đầu vào đã ingest, Bronze records | `...` |
| Số Silver records / rejected records | `...` / `...` |
| Số duplicate bị khử theo case demo | `...` |
| Batch ID và trip ID UPDATE; fare/tip trước → sau | `...` |
| Batch ID và trip ID INSERT; before 0 → after 1 | `...` |
| Batch ID SCHEMA, `surcharge_fee` | `...` |
| Silver version ban đầu / UPDATE / INSERT / SCHEMA | `...` |
| Kết quả pytest (passed / failed / skipped) | `...` |
| Evidence screenshot/log | `...` |

