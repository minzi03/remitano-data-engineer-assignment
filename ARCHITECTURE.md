# Data Warehouse Architecture – dbt Project

## 1. Data Warehouse Selection

Tôi lựa chọn **BigQuery** làm Data Warehouse để triển khai thực tế dự án này.

### Lý do lựa chọn BigQuery

#### 1.1 Serverless và tự động mở rộng
- Không cần quản lý hạ tầng
- Tự động tối ưu cho các workloads lớn
- Phù hợp với dữ liệu giao dịch crypto có tần suất cao

#### 1.2 Tối ưu cho analytical workloads (OLAP)
- Hỗ trợ tốt các phép tổng hợp theo ngày/tháng/quý
- Xử lý hiệu quả các phép join trên bảng fact lớn

#### 1.3 Chi phí dễ kiểm soát
- Tính phí theo lượng dữ liệu được scan
- Dễ tối ưu bằng partitioning hoặc clustering

#### 1.4 Tương thích tốt với dbt
- Hỗ trợ gốc trong dbt
- Dễ triển khai CI/CD và workflow pipelines

#### 1.5 Độ tin cậy cao và mở rộng linh hoạt
- Thích hợp cho các hệ thống dữ liệu tăng trưởng nhanh
- Không cần thay đổi kiến trúc khi mở rộng

---

## 2. Data Model Design: Bronze → Silver → Gold

Dự án sử dụng mô hình 3 lớp theo best-practice của dbt.

### 2.1 Cấu trúc dữ liệu

```
raw layer (dữ liệu sau ingestion – Bài 1)
├── raw.transactions
├── raw.users
└── raw.rates                     # Binance klines 1h

bronze / staging (models/staging/)
├── stg_transactions              # chuẩn hóa schema, ép kiểu, tạo transaction_hour
├── stg_users                     # chuẩn hóa bảng user
└── stg_rates                     # chuẩn hóa dữ liệu Binance

silver / intermediate (models/int/)
├── int_user_kyc_history          # SCD2: lịch sử KYC
├── int_rates_hourly_usd          # giá USD theo giờ
└── int_transactions_enriched     # transaction + tỷ giá + KYC tại thời điểm giao dịch

gold / marts (models/marts/)
└── fct_transactions_usd          # fact table phục vụ BI
```

### 2.2 Lý do chọn mô hình này

**Tách bạch trách nhiệm theo từng lớp**
- Bronze: chuẩn hóa dữ liệu thô
- Silver: xử lý business logic
- Gold: phục vụ BI, cung cấp dữ liệu sạch và dễ sử dụng

**Dễ bảo trì và mở rộng**
- Logic phức tạp tập trung ở Silver
- Khi yêu cầu nghiệp vụ thay đổi, chỉ cần chỉnh Silver layer

**Đảm bảo chất lượng dữ liệu**
- Tests nằm ở mỗi layer giúp đảm bảo tính toàn vẹn

---

## 3. Giải quyết yêu cầu quan trọng: Lịch sử KYC (SCD2)

### 3.1 Yêu cầu từ Team

> "Khi xem giao dịch 6 tháng trước, phải biết KYC level tại thời điểm giao dịch đó, không phải KYC hiện tại."

### 3.2 Giải pháp: Slowly Changing Dimension Type 2

**Bảng được sử dụng:** `int_user_kyc_history`

Các trường chính:
- `user_id`
- `kyc_level`
- `kyc_valid_from`
- `kyc_valid_to`

### 3.3 Logic join trong int_transactions_enriched

```sql
txn.created_at >= kyc_valid_from
AND (kyc_valid_to IS NULL OR txn.created_at < kyc_valid_to)
```

### 3.4 Kết quả

- Tạo ra trường `kyc_level_at_txn`, biểu thị chính xác KYC level tại thời điểm giao dịch
- Giúp truy vết lịch sử KYC một cách chính xác và đầy đủ
- Đáp ứng hoàn toàn yêu cầu nghiệp vụ số 3

---

## 4. dbt Materialization Strategy

Materialization được chọn theo vai trò từng layer để tối ưu hiệu năng và chi phí.

### 4.1 Bronze (Staging) – view

**Áp dụng cho:**
- `stg_transactions`
- `stg_users`
- `stg_rates`

**Lý do:**
- Chỉ chuẩn hóa schema, không cần lưu vật lý
- Dễ debug và phát triển
- Chi phí thấp

### 4.2 Silver (Intermediate) – view

(hoặc table khi dữ liệu lớn)

**Áp dụng cho:**
- `int_user_kyc_history`
- `int_rates_hourly_usd`
- `int_transactions_enriched`

**Lý do:**
- Chứa logic phức tạp (join, transform)
- Mặc định dùng view để linh hoạt khi phát triển

**Trong môi trường production:**
- Có thể chuyển `int_transactions_enriched` sang table hoặc incremental để tăng tốc

### 4.3 Gold (Marts) – table

**Áp dụng cho:**
- `fct_transactions_usd`

**Lý do:**
- BI query trực tiếp, yêu cầu hiệu năng cao
- Dữ liệu đã được xử lý hoàn chỉnh
- Tránh việc BI phải thực hiện các join tốn chi phí

**Khi dữ liệu lớn:**
- Có thể dùng incremental kết hợp partition theo `transaction_date`

---

## 5. Orchestration Pipeline (Daily)

Toàn bộ pipeline được orchestrate bằng **Apache Airflow** và chạy hàng ngày vào 01:00 UTC.

### 5.1 Bước 1 — Fetch Binance Rates

- Chạy script: `ingestion/fetch_rates.py`
- Gọi API Binance lấy dữ liệu klines 1h
- Lưu vào `raw_rates` hoặc nạp trực tiếp vào DWH

### 5.2 Bước 2 — Load Raw Data vào DWH

- Load `transactions.csv` → `raw.transactions`
- Load `users.csv` → `raw.users`
- Load rates → `raw.rates`

### 5.3 Bước 3 — dbt run (Bronze → Silver → Gold)

Chạy:
```bash
dbt run
```

dbt tự build theo dependency graph:
```
staging → intermediate → marts
```

### 5.4 Bước 4 — dbt test (Data Quality)

Chạy:
```bash
dbt test
```

Đảm bảo:
- Khoá chính unique
- Không null các trường quan trọng
- Các giá trị hợp lệ cho status

Nếu test fail → gửi cảnh báo.

### 5.5 DAG Dependency

```
fetch_rates
     ↓
load_raw_data
     ↓
dbt_run
     ↓
dbt_test
```

Pseudo-code trong Airflow:
```python
fetch_rates >> load_raw >> dbt_run >> dbt_test
```

---

## 6. Kết luận

Kiến trúc được đề xuất mang lại các lợi ích:

**Scalability:** BigQuery có khả năng mở rộng tự động.

**Maintainability:** Mô hình dữ liệu ba lớp rõ ràng và dễ bảo trì.

**Accuracy:** SCD2 đảm bảo truy vết lịch sử KYC chính xác.

**Performance:** Materialization tối ưu cho từng layer.

**Automation:** Airflow giúp vận hành pipeline tự động và đáng tin cậy.

Dự án đáp ứng đầy đủ cả ba bài yêu cầu: ingestion, transformation, và architectural design.
