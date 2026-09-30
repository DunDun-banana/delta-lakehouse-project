# Part B Task 3 Specification - Time Travel and Audit Verification

## 1. Mục tiêu

Task 3 chứng minh chuỗi quan hệ kỹ thuật sau trên bảng Silver Delta:

```text
MERGE -> Delta commit mới -> version mới -> snapshot cũ còn đọc được
      -> so sánh before/after -> audit bằng history và _delta_log
```

Workflow mặc định chỉ đọc. Task 3 không ghi vào Bronze, Silver, checkpoint hoặc
`_delta_log`, và không restore trực tiếp bảng Silver đang được Task 4 sử dụng.

## 2. Nguồn yêu cầu và hiện trạng

- Assignment yêu cầu đọc snapshot trước Silver MERGE bằng `versionAsOf`, sau đó
  audit bằng table history hoặc restore.
- Repository dùng PySpark `3.4.1` và `delta-spark 2.4.0` theo
  `requirements.txt`.
- Bảng cần audit theo convention hiện tại là `data/silver/taxi_trips`.
- Task 2 tạo Silver bằng initial write và các `MERGE`; một lần initial load có
  thể tạo nhiều version nên không được mặc định version 0 là baseline hoàn chỉnh.
- Notebook ghi lại evidence nghiệm thu ở các mốc 43-46. Các mốc này chỉ dùng để
  trình bày run hiện tại, không được hard-code vào implementation dùng lại.

## 3. Phạm vi

### Trong phạm vi

- Kiểm tra đường dẫn là Delta table và đọc history.
- Chọn commit UPDATE và INSERT từ `operationMetrics` thực tế.
- Loại commit có `metaData` khỏi candidate UPDATE thông thường khi có thể, để
  schema-evolution MERGE không che mất CDC UPDATE trước đó.
- Đọc snapshot ngay trước và ngay sau commit đã chọn.
- Tìm một `trip_id` thật sự thay đổi ở `fare_amount` hoặc `tip_amount`.
- Tìm một `trip_id` chỉ xuất hiện sau INSERT.
- Đọc JSON commit trong `_delta_log` và tóm tắt `commitInfo`, `add`, `remove`,
  `metaData`, `protocol`.
- Kiểm tra schema hiện tại và phát hiện commit giới thiệu `surcharge_fee` nếu log
  local còn giữ JSON tương ứng.
- Xuất console theo các section dễ trình bày và tùy chọn ghi evidence JSON nhỏ.
- Xác nhận version hiện tại không đổi từ đầu đến cuối audit.

### Ngoài phạm vi

- Restore trực tiếp bảng Silver hiện tại.
- VACUUM, OPTIMIZE, Z-ORDER hoặc Gold aggregation; đây là Task 4.
- Tuyên bố kết quả runtime khi workspace không có bảng Silver Delta.
- Coi synthetic `trip_id` là khóa nghiệp vụ chính thức của NYC TLC.

## 4. Quy tắc chọn version

1. Đọc `DeltaTable.forPath(...).history(limit)` và chuẩn hóa history theo
   `version`, `timestamp`, `operation`, `operationParameters` và
   `operationMetrics`.
2. Candidate UPDATE là commit `MERGE` có `numTargetRowsUpdated > 0`.
3. Candidate INSERT là commit `MERGE` có `numTargetRowsInserted > 0`.
4. Ưu tiên demo commit có một source row và đúng một row UPDATE/INSERT vì đây là
   pattern của script CDC hiện tại.
5. Với UPDATE, ưu tiên commit không chứa action `metaData` để tách CDC update khỏi
   schema evolution. Với INSERT, chọn candidate phù hợp mới nhất.
6. Snapshot trước commit `N` là `N - 1`; nếu `N <= 0` thì candidate không hợp lệ.
7. Người chạy có thể truyền `--update-version` hoặc `--insert-version`, nhưng code
   phải kiểm tra version đó tồn tại và metrics đúng loại yêu cầu.

Nếu không tìm được candidate, chương trình phải báo `NOT VERIFIED`, không thay
thế bằng số version đoán.

## 5. Thiết kế implementation

File chính: `src/audit/time_travel.py`.

Các nhóm hàm dự kiến:

- Delta API: `latest_commit`, `history`, `read_version`.
- History: chuẩn hóa row/metrics và chọn version audit.
- Comparison: tạo DataFrame before/after cho UPDATE và INSERT.
- Transaction log: đọc một commit JSON, đếm add/remove và lấy metadata có chọn
  lọc; không dump toàn bộ log.
- Verification: chạy audit end-to-end, in kết quả và trả evidence có thể ghi JSON.
- Restore: helper cũ có thể được giữ cho thao tác thủ công, nhưng CLI Task 3 không
  gọi restore trên Silver hiện tại.

Thiết kế đọc theo file của commit: MERGE trong Delta 2.4 là copy-on-write. File bị
`remove` ở commit `N` chính là một phần của snapshot `N - 1`; file được `add` chứa
dòng đã sửa hoặc dòng mới. Vì vậy UPDATE evidence so sánh hai nhóm file này, INSERT
evidence lấy key từ file mới rồi chỉ lọc key đó ở `versionAsOf N - 1`, thay vì join
hai snapshot khoảng 46 triệu dòng. Cách này chỉ bật khi `DESCRIBE DETAIL` cho thấy
bảng không partition và `minReaderVersion < 2` (không column mapping/deletion
vectors); nếu không, code tự quay về so sánh snapshot đầy đủ.

CLI dự kiến:

```powershell
python -m src.audit.time_travel `
  --silver data/silver/taxi_trips `
  --history 100 `
  --schema-column surcharge_fee `
  --output logs/task3_audit_rerun.json
```

Override chỉ dùng khi cần đối chiếu một run đã biết:

```powershell
python -m src.audit.time_travel `
  --silver data/silver/taxi_trips `
  --update-version 44 `
  --insert-version 45
```

## 6. Output console

```text
=== SILVER TABLE HISTORY ===
=== TIME TRAVEL: BEFORE MERGE ===
=== CURRENT SILVER VERSION ===
=== TIME TRAVEL: SNAPSHOT ROW COUNTS ===
=== CDC UPDATE AUDIT (file_scoped) ===
=== CDC INSERT AUDIT (file_scoped) ===
=== SCHEMA EVOLUTION VALUE ===
=== PROTOCOL AND TABLE DETAIL ===
=== DELTA TRANSACTION LOG ===
=== TASK 3 VERIFICATION ===
```

Evidence JSON chỉ chứa metadata, version và tối đa một record demo cho mỗi case;
không collect toàn bộ bảng Silver về driver.

## 7. Acceptance criteria

| Requirement | Điều kiện PASS |
|---|---|
| Time Travel | Đọc thành công snapshot `versionAsOf` ngay trước commit đã chọn |
| Version difference | History xác nhận before/after là hai version khác nhau |
| UPDATE evidence | Cùng `trip_id`, ít nhất một trường audit thay đổi |
| INSERT evidence | `trip_id` không có ở before và có ở after |
| History | Hiển thị version, timestamp, operation, parameters và metrics |
| Delta log | Đọc ít nhất một JSON commit liên quan, có commitInfo và add/remove |
| Protocol | Tìm thấy action `protocol` trong `_delta_log` và khớp `DESCRIBE DETAIL` |
| Schema | Schema hiện tại còn `surcharge_fee` khi Task 2 đã evolution |
| Schema value | Commit schema evolution ghi ít nhất một giá trị `surcharge_fee` khác null |
| Safety | Latest Silver version trước và sau audit bằng nhau |
| Reproducibility | Có command rõ ràng, version được discover hoặc override có validation |
| Integration | Không thay đổi Bronze/Silver/checkpoint; không cản Task 4 |

Nếu dữ liệu không có UPDATE hoặc INSERT phù hợp, kết quả phải là `NOT VERIFIED`
và nêu lý do thay vì tạo record giả.

## 8. Test plan

- Unit test parser của `operationMetrics` và version-selection rule.
- Unit test parser NDJSON `_delta_log` với `commitInfo`, `add`, `remove`,
  `metaData`, `protocol`.
- Unit test phát hiện commit thêm `surcharge_fee`.
- Static/import test trong môi trường không có PySpark để pure audit helpers vẫn
  kiểm tra được.
- Khi có Delta runtime và bảng local: chạy CLI hai lần; cả hai lần phải read-only
  và latest version phải giữ nguyên.
- Regression: chạy test Task 1-2 hiện có và smoke test Task 3.

## 9. Evidence nghiệm thu hiện tại

Hai run trên bảng thật `data/silver/taxi_trips` ngày 2026-09-30 đều trả về `PASS`
và được lưu trong Git tại `docs/evidence/` (bản gốc trong `logs/` bị ignore):

| File | Cách so sánh | Thời gian `run_audit` |
|---|---|---|
| `task3_audit_2026-09-30_baseline.json` | join toàn snapshot (code cũ) | khoảng 10 phút |
| `task3_audit_2026-09-30_rerun.json` | theo file của commit (code hiện tại) | 37,3 giây |

- UPDATE: version 43 -> 44, cùng `trip_id`
  `93184dd0b5a98cc1efe037f60a0ab8f5f90c178bbb12809ac61b991e106c9c05`, fare
  `26.8 -> 30.8`, tip `5.86 -> 7.86`, `record_source` `official_tlc -> cdc_demo`.
- INSERT: version 44 -> 45, trip
  `c10b59a0e29531186fd46a33b3bcde028387551dd7da7953c9f2139d2e2dbece`
  không có ở v44 và xuất hiện ở v45; số dòng `45,849,822 -> 45,849,823`.
- Schema: commit 46 thêm `surcharge_fee` vào `metaData`; bản ghi demo có
  `surcharge_fee = 1.5`.
- Protocol: action `protocol` ở commit 0 là `minReaderVersion = 1`,
  `minWriterVersion = 2`, khớp `DESCRIBE DETAIL` (không partition, 689 file).
- Commit 44 có 1 source row, 1 update, 1 add và 1 remove; commit 45 có 1 source
  row, 1 insert, 1 add và 0 remove; commit 46 có metadata, 1 update và 1 add/1
  remove.
- Latest version trước/sau audit đều là 46. Trước và sau run thứ hai, toàn bộ 533
  file `_delta_log`/checkpoint của Bronze, Silver, rejected, batch_audit giống hệt
  nhau, chứng minh chạy lại không thay đổi state.

Giải thích số liệu demo (đã đối chiếu Bronze và `batch_audit`):

- Update demo được append vào Bronze **hai lần**: v13 (fare 28.8, batch
  `demo_20260929T161715656026`) và v14 (fare 30.8, batch
  `demo_20260930T025850394775`). Cả hai rơi vào microbatch 44 (`incoming = 2`,
  `winners = 1`); dedup giữ bản mới nhất nên chênh lệch là `+4.0/+2.0`.
- Commit 46 không chỉ đổi schema: chế độ `schema` của `scripts/demo_silver_cdc.py`
  cũng tăng fare/tip, nên cùng trip đổi fare `30.8 -> 32.8`, tip `7.86 -> 8.86`.
- Commit 43 là bulk UPDATE 2,992 dòng của dirty fixture: fixture giữ `trip_id` của
  chuyến gốc tháng 1 nên khớp dòng đã có; fare/tip không đổi. Vì vậy audit không
  dùng v43 làm bằng chứng UPDATE.

## 10. Implementation đã hoàn thành

### Files

- `src/audit/time_travel.py`: CLI và các helper history, time travel,
  before/after comparison, `_delta_log` inspection và evidence JSON.
- `tests/test_time_travel.py`: unit test cho version discovery và Delta log parser.
- `tests/test_time_travel_spark.py`: smoke test Spark/Delta trên bảng tạm v0-v3,
  chỉ chạy khi `RUN_SPARK_TESTS=1`.
- `notebooks/03_time_travel_demo.ipynb`: notebook read-only gọi chung
  `run_audit`, hiển thị `DESCRIBE HISTORY`, `versionAsOf 0` và JSON thô trong
  `_delta_log`; không hard-code version để chạy.
- `docs/evidence/`: evidence JSON của các run nghiệm thu.

### Hàm chính

- `history`, `latest_commit`, `read_version`: Delta API portable.
- `select_audit_versions`: chọn UPDATE/INSERT từ metrics hoặc validate override.
- `changed_records`, `inserted_records`: tìm evidence ở mức `trip_id`.
- `inspect_delta_log_commit`: tóm tắt commitInfo/add/remove/metaData/protocol.
- `commit_file_paths`, `commit_action_preview`: file add/remove và JSON thô của
  một commit.
- `find_column_introduction`, `find_protocol_commit`: commit giới thiệu
  `surcharge_fee` và commit chứa action `protocol`.
- `file_scoped_update`, `file_scoped_insert`, `schema_value_record`: evidence chỉ
  đọc file của commit được chọn.
- `run_audit`: orchestration read-only và safety verification.

`restore_version` được giữ làm helper cho bản copy sandbox, nhưng không được expose
trong CLI và không được `run_audit` gọi.

## 11. Cách chạy và test

Môi trường: `.venv311` (Python 3.11, PySpark 3.4.1, delta-spark 2.4.0, Java 8),
chạy từ thư mục gốc repository và đã có Silver Delta local. Máy RAM thấp có thể
giảm driver memory:

```powershell
$env:SILVER_DRIVER_MEMORY = "2g"
.venv311\Scripts\python.exe -m src.audit.time_travel `
  --history 100 `
  --output logs\task3_audit_rerun.json
```

Không ghi đè evidence đã nghiệm thu; mỗi run nên dùng một file output riêng. Nếu
auto-discovery chọn commit khác với mốc nhóm muốn trình bày, inspect history trước
rồi mới override (code vẫn validate operation và metrics):

```powershell
.venv311\Scripts\python.exe -m src.audit.time_travel `
  --history 100 `
  --update-version 44 `
  --insert-version 45 `
  --output logs\task3_audit_override.json
```

Test:

```powershell
# Unit test thuần, không cần Spark
.venv311\Scripts\python.exe -m pytest -q tests
# Smoke test Spark/Delta trên bảng tạm v0-v3 (~1 phút)
$env:RUN_SPARK_TESTS = "1"
.venv311\Scripts\python.exe -m pytest -q tests\test_time_travel_spark.py
```

Kết quả ngày 2026-09-30: `pytest -q tests` cho 15 passed, 1 skipped (smoke test bị
skip mặc định); smoke test Spark/Delta pass trong 57 giây. Ba file
`test_bronze.py`, `test_silver.py`, `test_merge.py` vẫn là placeholder của Task 1-2.
Nghiệm thu trên bảng thật trả về `PASS` cho cả 9 check, tự chọn UPDATE v43 -> v44,
INSERT v44 -> v45, schema evolution ở v46 và giữ latest version ở v46.

## 12. Nội dung ngắn cho REPORT.md

```markdown
### Time Travel and Audit Verification

Mỗi thao tác `MERGE` thành công tạo một Delta commit và một table version mới.
Delta transaction log lưu các action cấu thành snapshot, vì vậy snapshot trước
MERGE vẫn đọc được bằng `versionAsOf` mà không cần copy toàn bộ bảng.

Task 3 đọc `DESCRIBE HISTORY` qua `DeltaTable.history()`, chọn commit UPDATE và
INSERT từ `operationMetrics`, rồi đọc version ngay trước/sau commit. Evidence
nghiệm thu cho thấy UPDATE v43 -> v44 giữ nguyên `trip_id` nhưng `fare_amount`
thay đổi 26.8 -> 30.8 và `tip_amount` thay đổi 5.86 -> 7.86; INSERT v44 -> v45
làm một `trip_id` mới xuất hiện (45,849,822 -> 45,849,823 dòng); commit v46 thêm
`surcharge_fee` vào schema và ghi giá trị 1.5 cho bản ghi demo.

JSON commit liên quan trong `_delta_log` cho thấy UPDATE ghi file mới và remove
file cũ, INSERT chỉ thêm file mới, còn schema evolution ghi action `metaData` với
schema mới; action `protocol` ở v0 là reader 1 / writer 2. Vì MERGE là
copy-on-write, audit chỉ đọc các file add/remove của commit được chọn (37 giây thay
vì khoảng 10 phút khi join hai snapshot). Audit không restore Silver; latest version
và toàn bộ file log/checkpoint được kiểm tra trước và sau, bảo đảm Task 3 không thay
đổi state mà Task 4 sử dụng.
```

Các số liệu trên đi kèm evidence đã commit trong `docs/evidence/`. Nếu run mới cho
version khác, cập nhật REPORT theo JSON mới.

## 13. Live demo 2-4 phút

Chạy trước cell 1 (khởi tạo Spark, khoảng 15 giây) khi nhóm bắt đầu thuyết trình;
toàn bộ notebook chạy khoảng 50 giây trên máy 8 GB RAM.

1. **Mở đầu (20 giây):** “Mỗi MERGE tạo một Delta version; Task 3 chứng minh
   snapshot cũ vẫn đọc được và transaction log giải thích thay đổi.”
2. **History (30 giây, cell 2):** `DESCRIBE HISTORY`; chỉ v44/v45/v46 có
   `source_rows = 1`, còn v43 là bulk update của dirty fixture. Timestamp hiển thị
   theo UTC (giờ Việt Nam +7).
3. **`versionAsOf 0` (20 giây, cell 3):** đúng cú pháp đề; v0 chưa có
   `surcharge_fee`, current có.
4. **UPDATE/INSERT (60 giây, cell 4):** cùng `trip_id` fare/tip before -> after;
   trip mới không có ở v44 và số dòng tăng 1 ở v45.
5. **Delta log (40 giây, cell 5):** JSON thô: `commitInfo`, `remove` + `add` ở
   UPDATE, chỉ `add` ở INSERT, `metaData` ở schema commit.
6. **Kết (20 giây, cell 6):** protocol reader 1 / writer 2, `non_destructive:
   true`, latest version trước/sau bằng nhau; không restore bảng Silver live.

Câu hỏi thường gặp: vì sao fare tăng +4 dù script chỉ cộng +2? Update demo được
append hai lần và cùng vào một microbatch, dedup giữ bản mới nhất (xem mục 9).

Nếu live runtime lỗi, dùng evidence JSON/notebook đã lưu làm backup nhưng phải nói
rõ đó là output của run trước, không trình bày như output vừa chạy.
