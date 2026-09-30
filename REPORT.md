# Project Report

## 1. Project Overview

This project demonstrates a Delta Lakehouse architecture built using the medallion pattern:

- Bronze: raw ingestion layer
- Silver: cleaning, validation, and CDC merge layer
- Gold: aggregated analytics layer

## 2. Team Information

- Team name: [Insert team name]
- Members: [Member 1], [Member 2], [Member 3], [Member 4], [Member 5], [Member 6]
- Mentor / instructor: [Insert name]

## 3. Objective

The main objective of this project is to design and implement a simple Lakehouse workflow that supports:

- raw data ingestion
- data quality checks
- incremental updates and merge logic
- analytics-ready gold tables
- storage optimization using Delta Lake features such as OPTIMIZE and Z-ORDER

## 4. Architecture Summary

The architecture follows a layered approach to improve data quality and maintainability:

- Bronze layer stores raw data as-is for traceability.
- Silver layer cleans, validates, and merges new data.
- Gold layer builds business-level metrics and reporting tables.
- Audit and optimization modules support time travel, storage efficiency, and performance comparison.

## 5. Medallion Flow

- Bronze
- Silver
- Gold

## 6. Data Processing Workflow

### Bronze

- ingest raw data
- preserve original source structure
- save to Delta table for traceability

### Silver

- clean invalid rows
- standardize schema and values
- handle duplicates and incremental updates

### Gold

- aggregate business metrics
- support analytics queries and reporting
- prepare simplified datasets for downstream consumers

### Time Travel and Audit Verification

Mỗi thao tác `MERGE` thành công tạo một Delta commit và một table version mới.
Delta transaction log lưu các action cấu thành snapshot, vì vậy snapshot trước
MERGE vẫn đọc được bằng `versionAsOf` mà không cần copy toàn bộ bảng.

Task 3 đọc history qua `DeltaTable.history()`, chọn commit UPDATE và INSERT từ
`operationMetrics`, rồi đọc version ngay trước/sau commit. Evidence nghiệm thu
ngày 2026-09-30 trong `docs/evidence/task3_audit_2026-09-30_rerun.json` cho thấy:

- UPDATE v43 -> v44 giữ nguyên `trip_id`, đổi `fare_amount` từ 26.8 thành 30.8
  và `tip_amount` từ 5.86 thành 7.86; metrics xác nhận đúng 1 row được update.
- INSERT v44 -> v45 làm một `trip_id` mới xuất hiện (45,849,822 -> 45,849,823
  dòng); metrics xác nhận đúng 1 row được insert.
- Commit v46 thêm `surcharge_fee` vào metadata của bảng và ghi giá trị 1.5 cho
  bản ghi demo (commit này đồng thời update fare/tip của cùng trip).
- Commit v43 là bulk UPDATE 2,992 dòng của dirty fixture, fare/tip không đổi, nên
  không được dùng làm bằng chứng CDC.

JSON commit trong `_delta_log` cho thấy UPDATE ghi file mới và remove file cũ,
INSERT chỉ thêm file mới, còn schema evolution ghi action `metaData` với schema
mới; action `protocol` ở v0 là reader 1 / writer 2. Vì MERGE là copy-on-write, file
bị remove ở commit N thuộc snapshot N-1, nên audit chỉ đọc các file add/remove của
commit được chọn: 37 giây thay vì khoảng 10 phút khi join hai snapshot khoảng 46
triệu dòng. Audit không restore Silver; latest version trước và sau đều là v46 và
toàn bộ file `_delta_log`/checkpoint không đổi sau khi chạy lại, nên Task 3 không
thay đổi state mà Task 4 sử dụng.

## 7. Storage Optimization Summary

This section should describe:

- file compaction approach
- data layout improvements
- partitioning or Z-ORDER strategy
- expected storage and query performance gains

## 8. Benchmark Notes

This section should document:

- baseline query performance
- optimized query performance
- comparison between before and after optimization
- conclusions from benchmark results

## 9. Risks and Limitations

- limited sample data size for demonstration
- no production-scale dataset used in this exercise
- performance results may vary depending on local environment
- schema changes must be managed carefully in Silver and Gold layers

## 10. Next Steps

- complete implementation for each task
- validate output tables and queries
- document test cases and benchmark results
- finalize presentation slides and project summary

## 11. Final Conclusion

This project provides a practical foundation for understanding Delta Lakehouse architecture, data pipeline design, and storage optimization. It is designed as a simple team-based learning project and can be extended with more realistic datasets and advanced pipeline logic in future iterations.
