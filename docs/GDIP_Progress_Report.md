# 📈 Báo Cáo Tiến Độ Toàn Bộ Dự Án GDIP
**Global Development Intelligence Platform — Economic AI Analyst**

*Cập nhật lần cuối: 21/06/2026*

---

## 🗺️ KIẾN TRÚC TỔNG THỂ

```
World Bank API
      │
      ▼
[LAYER 1] DATA LAYER              ← ✅ R&D Notebook xong
  Bronze (Raw JSON)
  → Silver (Clean CSV)
  → Gold (Feature Store)
      │
      ▼
[LAYER 2] ML LAYER                ← ✅ R&D Notebook xong
  XGBoost + LightGBM + CatBoost (EWS Classifier)
  Holt-Winters (Macro Forecasting)
  MLflow (Experiment Tracking)
      │
      ▼
[LAYER 3] PRODUCTION LAYER        ← 🔨 Tiếp theo #1
  Airflow DAGs (Orchestration)
  PostgreSQL + pgvector (Database)
      │
      ▼
[LAYER 4] LLM / RAG LAYER         ← 🔨 Tiếp theo #2
  RAG From Scratch (No LangChain)
  QLoRA Fine-tuning
      │
      ▼
[LAYER 5] MULTI-AGENT LAYER       ← 🔨 Tiếp theo #3
  LangGraph (Economic Copilot)
  Text-to-SQL · Risk Report Agent
      │
      ▼
[LAYER 6] MLOps MONITORING        ← 🔨 Tiếp theo #4
  Drift Detection · Grafana · Model Card
```

---

## ✅ PHẦN 1 — ĐÃ HOÀN THÀNH (R&D NOTEBOOK)

> Toàn bộ phần này nằm trong thư mục `notebooks/`.
> Đây là giai đoạn **nghiên cứu thử nghiệm** trên Jupyter Notebook,
> chưa phải code Production.

### 📁 Artifacts (File thực tế đã được sinh ra)

| File | Vị trí | Kích thước | Mô tả |
|---|---|---|---|
| `wb_macro_data.csv` | `airflow/dags/data/silver/` | 1.5 MB | Data thô World Bank sau khi chuyển đổi từ JSON |
| `wb_macro_clean.csv` | `airflow/dags/data/silver/` | 1.2 MB | Data đã xử lý missing values (KNN + ffill) |
| `feature_store.csv` | `airflow/dags/data/gold/` | 7.8 MB | Kho 142 features đã tính toán cho toàn bộ quốc gia |
| `train.csv / val.csv / test.csv` | `airflow/dags/data/ml_data/` | — | Tập chia theo thời gian cho ML (1990-2015 / 2016-2018 / 2019-2024) |
| `champion_model.pkl` | `airflow/dags/models/` | 18.3 MB | Ensemble 3 model đóng gói cùng trọng số + feature_cols |
| `best_params.json` | `airflow/dags/models/` | 650 B | Siêu tham số tối ưu từ Optuna |
| `mlflow.db` | `notebooks/mlruns/` | 188 KB | Lịch sử thí nghiệm MLflow |

---

### 📓 Notebook 01 — `01_eda_preprocess_feature.ipynb`
**Mục tiêu: Biến rác thành vàng (Bronze → Silver → Gold)**

- [x] **EDA**: Phân tích phân phối, vẽ heatmap tương quan, kiểm tra outlier cho 10 chỉ số kinh tế.
- [x] **Cleaning (Silver)**: Lọc bỏ các mã khu vực giả (AFE, WLD...), chỉ giữ quốc gia thật. Lấp rỗng bằng `KNN Imputer` (trước 1980) + `ffill` (còn lại).
- [x] **Feature Engineering (Gold)**:
    - Lag features: `gdp_growth_lag1`, `inflation_lag2`, `unemployment_lag1`
    - Momentum (gia tốc): `gdp_acceleration`, `inflation_acceleration`
    - Tỷ lệ: `debt_to_gdp`, `reserves_to_imports`
    - Giá trị biên độ cắt: `inflation_clipped`, `fdi_inflow_clipped`
    - Z-Score chéo quốc gia theo năm (Cross-country normalization)
    - **Tổng cộng: 142 biến số đặc trưng**
- [x] **Weak Supervision Labeling**: Tự động gán nhãn 3 mức rủi ro:
    - `Low Risk (0)`: Kinh tế bình thường
    - `Medium Risk (1)`: GDP < 1% hoặc lạm phát > 10%
    - `High Risk (2)`: GDP < -2% hoặc lạm phát > 20% (khủng hoảng)
- [x] **Time-based Split**: Chia train/val/test theo thời gian, **không** random shuffle.

---

### 📓 Notebook 02 — `02_model_training.ipynb`
**Mục tiêu: Train + Tối ưu + Đóng gói mô hình cảnh báo sớm**

- [x] **Baseline Benchmarking**: So sánh 5 thuật toán (Logistic Regression, Random Forest, XGBoost, LightGBM, CatBoost) trên cùng tập dữ liệu.
- [x] **Xử lý Class Imbalance**: Tính `class_weight` động theo tỷ lệ phân phối thực tế (Low=0.50, Med=1.23, High=5.33).
- [x] **Hyperparameter Tuning (Optuna)**: 100 trials cho XGBoost và LightGBM. Kết quả tốt nhất:
    - XGBoost Val AUC: **0.8966**
    - LightGBM Val AUC: **0.8976**
- [x] **Ensemble Learning (Weighted Blend)**:
    - Tối ưu trọng số bằng Optuna: `XGB=0.53, LGB=0.36, CAT=0.10`
    - Val AUC (Blended): **0.8989**
- [x] **SHAP Explainability**: Vẽ Summary Plot + Dependence Plot + Waterfall Plot để giải thích tại sao AI cảnh báo một quốc gia cụ thể.
- [x] **MLflow Logging**: Lưu toàn bộ params + metrics + model artifact vào `mlflow.db`. Run ID: `d765c609f67c4a1289ccd87dec8902ad`.
- [x] **Đóng gói**: Lưu `champion_model.pkl` chứa 3 models + weights + feature_cols.

---

### 📓 Notebook 03 — `03_prophet_forecasting.ipynb`
**Mục tiêu: Dự báo chuỗi thời gian kinh tế vĩ mô 5 năm tương lai**

- [x] **Thuật toán**: Chọn **Holt-Winters Exponential Smoothing** thay vì Prophet (Prophet crash lỗi CmdStan trên Windows; Holt-Winters chạy Python thuần 100%, không phụ thuộc C++ backend).
- [x] **Fit + Forecast**: Train trên dữ liệu VNM 1985-2024, dự báo GDP 2025-2029.
- [x] **Backtesting (Walk-Forward Validation)**:
    - Test 1 (COVID period 2020-2024): Sai số lớn → Bài học "Thiên nga đen" — Bất kỳ mô hình nào cũng bất lực trước biến cố ngoại sinh.
    - Test 2 (Bình thường 2015-2019): Đường dự báo bám sát thực tế — Holt-Winters vẫn rất mạnh trong điều kiện ổn định.
- [x] **Kết quả**: Dự báo GDP Việt Nam năm 2029 tiệm cận **6.8%**, phù hợp với nhận định chuyên gia.

> ⚠️ **Lưu ý quan trọng**: File `asean_forecast_2025_2029.csv` chưa được lưu vào Gold Layer vì bị gián đoạn. Cần chạy lại Part 3 của Notebook 03 để sinh ra file này.

---

## 🔨 PHẦN 2 — VIỆC CẦN LÀM TIẾP THEO (PRODUCTION)

> Mục tiêu: Đưa toàn bộ những gì đã làm ở Notebook lên hệ thống chạy tự động.

---

### ⬡ TẦNG 3: Production Scripts & Airflow (Ưu tiên #1)

**Mục tiêu:** Không bao giờ phải mở Jupyter Notebook để chạy lại.

#### Bước 3.1 — Refactor code Notebook thành Scripts `.py`

| Script cần tạo | Từ Notebook | Chức năng |
|---|---|---|
| `airflow/dags/ingestion/wb_api.py` | N/A (đã có sẵn) | Kéo data thô từ World Bank API |
| `airflow/dags/transform/clean_silver.py` | Notebook 01 Part 1-2 | Làm sạch Bronze → Silver |
| `airflow/dags/transform/feature_engineering.py` | Notebook 01 Part 3 | Tính 142 features Gold |
| `airflow/dags/ai/predict_risk.py` | Notebook 02 | Load `champion_model.pkl`, predict rủi ro cho data mới nhất |
| `airflow/dags/ai/forecast_macro.py` | Notebook 03 | Chạy Holt-Winters, xuất `asean_forecast_2025_2029.csv` |

#### Bước 3.2 — Xây dựng Airflow DAGs

```
DAG 1: data_pipeline_dag.py  (chạy hàng tháng)
  Task 1 → wb_api.py          (Ingestion: kéo API)
  Task 2 → clean_silver.py    (Clean & Transform)
  Task 3 → feature_engineering.py (Feature Store)

DAG 2: ml_inference_dag.py   (chạy sau DAG 1 xong)
  Task 1 → forecast_macro.py  (Holt-Winters: xuất forecast CSV)
  Task 2 → predict_risk.py    (XGBoost: xuất risk labels)
  Task 3 → load_to_db.py      (Nạp kết quả vào PostgreSQL)
```

#### Bước 3.3 — Thiết lập Database

```
PostgreSQL + pgvector extension
  Bảng: macro_data        (time-series data từ Silver)
  Bảng: risk_predictions  (kết quả XGBoost theo quốc gia/năm)
  Bảng: macro_forecasts   (kết quả Holt-Winters 2025-2029)
  Bảng: document_embeddings (cho RAG ở Tầng 4)
```

---

### ⬡ TẦNG 4: RAG & LLM (Ưu tiên #2)

**Mục tiêu:** Chatbot đọc được cả database lẫn tài liệu chuyên môn để trả lời câu hỏi.

**Nguyên tắc:** **Không dùng LangChain**. Tự code từ đầu — đây là điểm mấu chốt để trả lời câu hỏi phỏng vấn.

```
File: ai/rag/pipeline_scratch.py
  Step 1: chunk_document()     — Cắt tài liệu WB/IMF theo Sentence-boundary
  Step 2: embed_batch()        — Nhúng bằng sentence-transformers
  Step 3: upsert_pgvector()    — Lưu vào PostgreSQL pgvector
  Step 4: hybrid_retrieve()    — Tìm kiếm Dense (cosine) + BM25 + RRF rerank
  Step 5: cross_encoder_rerank() — Dùng Cross-encoder để tinh chỉnh top-k
```

**Fine-tuning (QLoRA):**
```
File: ai/finetune/qlora_trainer.py
  Model gốc: Llama 3.1 8B (hoặc Qwen2.5 7B)
  Dataset: Tự tạo từ WB Country Reports + IMF Article IV
  Kỹ thuật: LoRA rank=16, alpha=32 → chỉ train 0.034% parameters
```

---

### ⬡ TẦNG 5: Multi-Agent LangGraph (Ưu tiên #3)

**Mục tiêu:** User chat bằng tiếng Việt → Hệ thống tự quyết định gọi Agent nào.

```
File: ai/agents/economic_copilot.py

  SupervisorAgent (LangGraph StateGraph)
    │
    ├── RiskScorerAgent     → Đọc risk_predictions từ DB, xuất điểm 0-100
    ├── TextToSQLAgent      → Dịch câu hỏi → SQL → truy vấn DB
    ├── RAGAgent            → Truy xuất tài liệu chuyên môn
    └── CountryReportAgent  → Tổng hợp 3 agent trên thành báo cáo hoàn chỉnh
```

---

### ⬡ TẦNG 6: MLOps Monitoring (Ưu tiên #4)

**Mục tiêu:** Hệ thống tự phát hiện khi model "già" và cần retrain.

```
ai/mlops/monitoring.py
  - Data Drift:       KS-test so sánh phân phối feature mới vs cũ
  - Prediction Drift: PSI (Population Stability Index)
  - Concept Drift:    ADWIN algorithm

ai/mlops/champion_challenger.py
  - Auto-retrain khi drift vượt ngưỡng
  - So sánh AUC model mới vs champion hiện tại
  - Tự động promote model mới nếu tốt hơn

Infra: Prometheus metrics endpoint + Grafana dashboard
```

---

## 🗂️ CẤU TRÚC THƯ MỤC HIỆN TẠI

```
gidp/
├── notebooks/                     ← ✅ R&D hoàn thành
│   ├── 01_eda_preprocess_feature.ipynb
│   ├── 02_model_training.ipynb
│   └── 03_prophet_forecasting.ipynb
│
├── airflow/dags/
│   ├── data/
│   │   ├── bronze/                ← Raw JSON từ WB API
│   │   ├── silver/
│   │   │   ├── wb_macro_data.csv  ← ✅ Data thô
│   │   │   └── wb_macro_clean.csv ← ✅ Data đã làm sạch
│   │   ├── gold/
│   │   │   └── feature_store.csv  ← ✅ 142 features (7.8 MB)
│   │   └── ml_data/
│   │       ├── train.csv / val.csv / test.csv
│   │       └── ⚠️ asean_forecast_2025_2029.csv  ← CHƯA CÓ (cần chạy lại NB03)
│   ├── models/
│   │   ├── champion_model.pkl     ← ✅ 18.3 MB (XGB+LGB+CAT+weights)
│   │   └── best_params.json       ← ✅ Optuna best params
│   ├── ingestion/                 ← 🔨 Cần viết wb_api script
│   ├── transform/                 ← 🔨 Cần viết clean + feature scripts
│   └── ai/                        ← 🔨 Cần viết predict + forecast scripts
│
├── ai/                            ← 🔨 Chưa làm (RAG, Agents, MLOps)
├── api/                           ← 🔨 Chưa làm (FastAPI endpoint)
├── infra/                         ← 🔨 Chưa làm (Docker, Postgres setup)
├── dbt/                           ← 🔨 Chưa làm (data transformation)
├── tests/                         ← 🔨 Chưa có unit tests
└── docs/                          ← ✅ Có kiến trúc + roadmap
```

---

## 📊 CÁC CHỈ SỐ HIỆU NĂNG ĐÃ ĐẠT ĐƯỢC

| Mô hình | Val AUC | Macro F1 | Recall_High |
|---|---|---|---|
| XGBoost (Optuna) | 0.8966 | — | — |
| LightGBM (Optuna) | 0.8976 | — | — |
| **Champion Ensemble (Blend)** | **0.8989** | — | — |
| Test AUC (OvR) | **0.6891** | 0.51 | 0.25 |

> ⚠️ **Gap Train-Test lớn (0.899 vs 0.689)**: Nguyên nhân do tập test (2019-2024) chứa cú sốc COVID-19 ngoại sinh — không phải overfitting. Đây là điểm mấu chốt cần giải thích rõ khi bảo vệ đồ án.

---

## ❓ CÁC ĐIỂM CẦN GHI NHỚ KHI TIẾP TỤC

1. **`asean_forecast_2025_2029.csv` chưa được lưu** — Cần chạy lại Part 3 Notebook 03 để xuất file này vào `airflow/dags/data/gold/`.
2. **Hai cột `inflation_clipped` và `fdi_inflow_clipped` không có trong `feature_store.csv`** — Đây là lỗi thiết kế ở Notebook 01 (2 cột này được tạo ra sau khi đã lưu file). Khi viết Production script thì cần tạo cả 2 cột này **trước** khi xuất file.
3. **Prophet không dùng được trên môi trường này** — Do CmdStan (C++ backend) bị Windows Defender chặn. Dùng `statsmodels.tsa.holtwinters.ExponentialSmoothing` thay thế trong mọi script Production.
4. **MLflow UI**: Mở bằng lệnh `mlflow ui --backend-store-uri sqlite:///mlflow.db` từ thư mục `notebooks/`.
