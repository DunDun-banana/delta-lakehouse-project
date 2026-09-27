# CDC Demo — Silver Version Tracking

## Task 2: Silver Layer & CDC

| Mốc thực hiện           | Silver Version | trip_id & ingested batch id   | Ghi chú                          |
| ----------------------- | -------------- | --------- | -------------------------------- |
| Snapshot Silver hoàn chỉnh sau intial load 12 months| 44      | —         | Bronze → Silver, chưa chạy CDC   |
| Sau CDC UPDATE          | 45      | **trip_id**=93184dd0b5a98cc1efe037f60a0ab8f5f90c178bbb12809ac61b991e106c9c05, **ingest_batch_id**=demo_20260927T080446776977 | Thay đổi fare_amount, tip_amount |
| Sau CDC INSERT          | 47      | **new_trip_id**=33f764b320f526509a5ffc671a628fff2361a066ceeae606bb7f39abd528623f, **ingest_batch_id**=demo_20260927T085807873071  | Thêm bản ghi mới                 |
| Sau Schema Evolution    | 48      | —         | Thêm cột surcharge_fee           |

## Bằng chứng

* Silver Delta Log: `data/silver/taxi_trips/_delta_log/`
* Streaming Checkpoint: `data/checkpoints/silver_taxi/commits/`
* Batch Audit: `data/silver/batch_audit/`
* Exported History: `logs/silver_version_history.json`

## Trạng thái

* [x] Initial load hoàn thành
* [x] CDC UPDATE
* [x] CDC INSERT
* [x] Schema Evolution

