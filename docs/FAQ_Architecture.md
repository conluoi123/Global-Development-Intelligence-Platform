# GDIP — Q&A: Kiến trúc Dữ liệu (Data Architecture)

Tài liệu này giải đáp các câu hỏi quan trọng về quyết định thiết kế hệ thống (Architecture Design Decisions) thường gặp khi phỏng vấn vị trí Data Engineer hoặc AI Engineer.

---

## 1. Tại sao ở tầng Bronze lại lưu JSON (hoặc raw string) mà không lưu thẳng thành `.parquet`?

Trong kiến trúc của GDIP, tầng Bronze (Raw) cố tình lưu trữ dữ liệu dưới dạng JSON/String thô (raw payload) thay vì ép ngay vào định dạng cột như Parquet. Quyết định này dựa trên các nguyên tắc thiết kế cho môi trường Production:

1. **Schema Evolution (Thay đổi cấu trúc bất ngờ):** Dữ liệu lấy từ API bên thứ 3 (World Bank) có thể thay đổi cấu trúc bất cứ lúc nào (thêm field mới, đổi tên field, thay vì trả về `int` lại trả về `string` có chứa text). 
   - **JSON (Schema-on-read):** Chấp nhận mọi thứ. Pipeline vẫn chạy thành công và lưu trữ được data mới.
   - **Parquet (Schema-on-write):** Đòi hỏi strict schema. Nếu API trả về khác schema định sẵn, pipeline sẽ crash (Fail) ngay lập tức, gây mất dữ liệu của ngày hôm đó.
2. **Khả năng Debugging & Audit:** Tầng Bronze là "Single Source of Truth". Lưu nguyên bản raw JSON giúp chúng ta có bằng chứng chính xác API đã trả về cái gì. Nếu data ở tầng Silver bị sai, ta luôn có thể quay lại raw JSON để re-process (chạy lại) mà không cần gọi lại API.
3. **Thực tế GDIP:** Hệ thống sử dụng **Delta Lake** cho tầng Bronze, trong đó cột `raw_payload` chứa chuỗi JSON nguyên bản. Điều này vừa tận dụng được tính ACID của Delta Lake, vừa giữ được sự linh hoạt của JSON.

---

## 2. Tại sao đồ án này dùng PostgreSQL mà lại không dùng DuckDB (hoặc ngược lại)?

Một Senior Engineer hiểu rằng không có công cụ nào hoàn hảo cho mọi việc. Trong GDIP, **cả hai công cụ này đều được sử dụng nhưng cho mục đích hoàn toàn khác biệt (Separation of Concerns)**:

### PostgreSQL (Tối ưu cho OLTP - Xử lý giao dịch & Metadata)
Được sử dụng cho các hệ thống cần nhiều luồng kết nối (concurrent reads/writes) và làm backend cho ứng dụng:
- **Airflow Metadata DB:** Quản lý trạng thái hàng ngàn task, trigger, logs.
- **MLflow Backend:** Lưu trữ param, metrics của hơn 2000+ AI models.
- **pgvector:** Dùng PostgreSQL extension để làm Vector Database lưu trữ embeddings cho tính năng RAG.
> *DuckDB không phù hợp làm backend server vì nó lock database file khi write, không hỗ trợ concurrent write tốt như Postgres.*

### DuckDB (Tối ưu cho OLAP - Xử lý phân tích & Transform)
Được sử dụng kết hợp với **dbt** để xử lý dữ liệu (Transform) từ Bronze -> Silver -> Gold.
- Nó hoạt động như một Engine phân tích in-process. Thay vì phải setup cả một cụm Apache Spark cồng kềnh, DuckDB xử lý dữ liệu dạng cột (columnar) siêu tốc trên 1 máy tính đơn lẻ (nhanh hơn Spark 3-5 lần cho dữ liệu dưới 50GB).

---

## 3. Phân biệt Data Lake và Data Warehouse

Trong GDIP, việc phân chia Data Lake và Data Warehouse được thể hiện rõ qua kiến trúc Medallion.

### Data Lake (Hồ dữ liệu)
- **Định nghĩa:** Nơi lưu trữ tập trung MỌI LOẠI dữ liệu ở dạng nguyên bản (thô).
- **Cấu trúc:** Có thể là dữ liệu có cấu trúc (bảng biểu), bán cấu trúc (JSON, XML), hoặc phi cấu trúc (File PDF báo cáo của World Bank, Hình ảnh).
- **Đặc điểm:** Schema-on-read (Lưu trước, khi nào đọc mới gán cấu trúc). Chi phí lưu trữ rẻ.
- **Trong GDIP:** Tương đương với hệ thống **MinIO** và **Tầng Bronze**.

### Data Warehouse (Kho dữ liệu)
- **Định nghĩa:** Nơi lưu trữ dữ liệu đã được làm sạch, xử lý và chuẩn hóa, sẵn sàng phục vụ cho phân tích kinh doanh (BI) và Machine Learning.
- **Cấu trúc:** Dữ liệu hoàn toàn có cấu trúc chặt chẽ (Tables, Rows, Columns, Foreign Keys).
- **Đặc điểm:** Schema-on-write (Phải có cấu trúc bảng rõ ràng mới được ghi vào). Tối ưu cực tốt cho các câu lệnh SQL truy vấn phức tạp (JOIN, GROUP BY).
- **Trong GDIP:** Tương đương với **Tầng Gold** (chứa các Fact & Dimension tables, Feature Store).

> **Bonus Insight:** GDIP thực chất đang xây dựng một **Data Lakehouse** (thông qua Delta Lake). Nó mang ưu điểm của cả hai: Vừa lưu trữ rẻ, linh hoạt nhiều loại file như Data Lake, vừa hỗ trợ ACID (Transactions) và hiệu năng truy vấn cao như Data Warehouse.

---

## 4. Giải thích về tính chất ACID của Delta Lake trong dự án

Dự án này sử dụng **Delta Lake** thay vì lưu file Parquet hay CSV thông thường trên MinIO. Điểm khác biệt lớn nhất khiến Delta Lake đạt chuẩn Production chính là nó đảm bảo được **tính chất ACID**, giải quyết các vấn đề đau đầu nhất của Data Engineer:

### A - Atomicity (Tính nguyên tử)
- **Vấn đề cũ:** Khi đang ghi 100 file data vào hồ dữ liệu mà bị mất điện giữa chừng, bạn sẽ có 50 file bị lỗi, dữ liệu bị "nửa mùa" và hỏng hoàn toàn tập dữ liệu.
- **Delta Lake giải quyết:** Một giao dịch (transaction) ghi dữ liệu **hoặc là thành công 100%, hoặc là thất bại toàn bộ**. Nếu Airflow task bị sập giữa chừng khi đang ghi data, Delta Lake sẽ tự động rollback (hoàn tác) như chưa hề có chuyện gì xảy ra. Sẽ không bao giờ có chuyện lưu lại những file bị lỗi một nửa.

### C - Consistency (Tính nhất quán)
- **Vấn đề cũ:** Nếu có một người đang đọc data cùng lúc với một pipeline đang ghi data mới vào, người đọc có thể nhận được kết quả sai hoặc bị crash.
- **Delta Lake giải quyết:** Đảm bảo người đọc luôn nhìn thấy một phiên bản dữ liệu hợp lệ và trọn vẹn nhất (Snapshot Isolation). Trong GDIP, khi dbt đang update bảng Silver, AI model đang truy vấn data đó vẫn sẽ đọc được bản dữ liệu cũ hoàn hảo cho đến khi dbt update xong 100%.

### I - Isolation (Tính độc lập)
- **Vấn đề cũ:** Hai pipeline cùng lúc cố gắng ghi đè hoặc sửa cùng một bảng dữ liệu sẽ gây ra xung đột (conflict) làm hỏng file.
- **Delta Lake giải quyết:** Delta Lake xử lý tốt việc nhiều quá trình thao tác (read/write) diễn ra song song trên cùng một bảng. Các transaction được cô lập hoàn toàn. Nếu có xung đột (ví dụ 2 task cùng update 1 row), Delta Lake sẽ báo lỗi rõ ràng chứ không âm thầm làm hỏng file data.

### D - Durability (Tính bền vững)
- **Vấn đề cũ:** Sợ mất dữ liệu sau khi ghi xong.
- **Delta Lake giải quyết:** Một khi dữ liệu được báo là "đã ghi thành công" (commit) vào transaction log của Delta Lake (được lưu trên MinIO), nó sẽ tồn tại mãi mãi và an toàn cho dù hệ thống có bị sập ngay giây tiếp theo. 

> **Tóm tắt ứng dụng thực tế trong GDIP:** Nhờ ACID của Delta Lake, chúng ta có thể tự tin đặt thuộc tính `'delta.appendOnly' = 'true'` cho tầng Bronze để đảm bảo dữ liệu thô **bất biến (immutable)**. Đồng thời, tính năng **Time Travel** (đọc lại dữ liệu của ngày hôm qua) của Delta Lake cũng được hưởng lợi trực tiếp từ Transaction Log của ACID.

---

## 5. Ngoài ACID, Delta Lake còn mang lại lợi ích thực tế gì cho GDIP?

Bên cạnh tính nguyên vẹn (ACID), việc sử dụng Delta Lake trong đồ án giúp giải quyết các bài toán "đau đầu" khác trong vận hành data pipeline:

### 5.1. Time Travel (Du hành thời gian & Phục hồi dữ liệu)
Mọi thay đổi trên Delta Lake đều được lưu lại trong một file gọi là **Transaction Log (`_delta_log`)**.
- **Use case trong đồ án:** Giả sử một lỗi trong code dbt làm sai lệch toàn bộ dữ liệu ở tầng Silver ngày hôm nay. Với Parquet bình thường, bạn phải chạy lại pipeline từ đầu. Với Delta Lake, bạn chỉ cần gõ 1 câu lệnh để "quay ngược thời gian" về bản snapshot của ngày hôm qua: `RESTORE TABLE silver.indicators TO TIMESTAMP AS OF '2024-01-01'`.

### 5.2. Schema Enforcement & Schema Evolution
- **Schema Enforcement (Ép kiểu nghiêm ngặt):** Khi ghi dữ liệu từ Bronze sang Silver, Delta Lake sẽ từ chối thẳng thừng (fail-fast) nếu phát hiện dữ liệu kiểu `String` cố tình ghi vào cột kiểu `Int`. Điều này giữ cho data luôn sạch sẽ.
- **Schema Evolution (Tiến hóa cấu trúc):** Nếu API World Bank trả về một cột MỚI hoàn toàn (ví dụ: `sustainability_score`) và bạn muốn giữ nó lại. Delta Lake cho phép tự động gộp thêm cột mới này vào bảng hiện tại mà không làm hỏng dữ liệu cũ, chỉ cần bật tính năng `mergeSchema`.

### 5.3. Z-Ordering & Data Skipping (Tối ưu truy vấn)
Khi data lớn lên (vd: hàng trăm triệu dòng cho tất cả các quốc gia trong 60 năm), việc query sẽ bị chậm.
- Delta Lake tự động thu thập metadata (giá trị min, max của từng cột trong từng file Parquet).
- Khi user truy vấn `WHERE country_code = 'VNM'`, Delta Lake sẽ dùng kỹ thuật **Data Skipping** để bỏ qua hàng ngàn file không chứa dữ liệu Việt Nam, giúp tốc độ truy vấn ở tầng Gold siêu nhanh mà không cần phải quét toàn bộ ổ cứng.
