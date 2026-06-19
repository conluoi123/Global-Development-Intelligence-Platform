# GDIP — Tổng Quan Định Hướng Dự Án

> **Mục tiêu cuối cùng:** Build một AI Platform phân tích kinh tế thế giới
> dựa trên dữ liệu World Bank — đủ để xin **AI Engineer / ML Engineer Intern** cuối 2026.

---

## Tại Sao Dự Án Này Tồn Tại?

Hầu hết đồ án sinh viên dừng lại ở mức: *"Tôi lấy data rồi train model."*

GDIP được thiết kế để trả lời câu hỏi phỏng vấn thực tế:
- *"Bạn xử lý missing data như thế nào?"*
- *"Tại sao không dùng LangChain?"*
- *"Supervisor agent handle conflict giữa 2 sub-agent thế nào?"*
- *"Model của bạn có bị data leakage không? Chứng minh?"*

---

## Bức Tranh Tổng Thể

```
World Bank API
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1 — DATA (Đang làm)                                  │
│                                                             │
│  Bronze (Raw JSON) → Silver (Clean CSV) → Gold (Features)  │
│  Airflow điều phối · dbt transform · Great Expectations QC  │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2 — MACHINE LEARNING (Tháng 1)                       │
│                                                             │
│  Feature Engineering → XGBoost Classifier → SHAP + Calib   │
│  Walk-forward CV · Benchmark table · Prophet Forecasting    │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3 — LLM (Tháng 2)                                   │
│                                                             │
│  RAG from scratch (không LangChain) · QLoRA Fine-tuning     │
│  pgvector · BM25 + Dense Hybrid Search · Cross-encoder      │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 4 — AI AGENTS (Tháng 3 — Phase 3)                   │
│                                                             │
│  LangGraph Multi-Agent · Economic Copilot                   │
│  Risk Scoring Agent · Country Research Report               │
│  Text-to-SQL (schema-constrained)                           │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 5 — MLOps (Phase 4)                                  │
│                                                             │
│  Model Card YAML · Champion-Challenger · 3 loại Drift       │
│  MLflow Registry · Prometheus + Grafana monitoring          │
└─────────────────────────────────────────────────────────────┘
```

---

## Các Bước Cụ Thể Theo Thứ Tự

### ✅ Đã làm (Foundation)
- [x] Cấu trúc thư mục chuẩn Production
- [x] `ingestion/wb_api.py`: Kéo 10 chỉ số kinh tế từ WB API
- [x] `transform/to_csv.py`: Chuyển JSON Bronze → CSV Silver
- [x] `.gitignore`, `requirements.txt`, `.env.example`
- [x] Docs kiến trúc + FAQ

---

### 🔨 Bước Tiếp Theo (Làm ngay)

#### Bước 1 — Hoàn thiện Data Layer (1-2 ngày)
```
Mục tiêu: Có một file CSV sạch, đủ dùng để train ML

Việc cần làm:
1. Lọc bỏ các "khu vực" (AFE, WLD...), chỉ giữ quốc gia thật
2. Xử lý Missing Values (forward fill hoặc KNN)
3. Lưu ra gold/feature_store.csv (14,000+ dòng x 10 cột)

File: airflow/dags/transform/to_silver.py (sửa lại to_csv.py)
```

#### Bước 2 — Feature Engineering (3-5 ngày)
```
Mục tiêu: Từ 10 chỉ số thô → 50-80 features chất lượng

Tạo thêm:
- Lag features:    gdp_growth_lag1, inflation_lag2, ...
- Ratio features:  debt_to_gdp, reserves_to_imports
- Momentum:        gdp_acceleration (diff của diff)
- Cross-country z-score: normalized theo năm

File: ai/classifier/feature_engineering.py
```

#### Bước 3 — ML Classifier (1 tuần)
```
Mục tiêu: Tự label "khủng hoảng kinh tế" và train model

Weak supervision labeling:
  crisis = GDP < -2% OR inflation > 20% OR FX crash > 30%
  Validate bằng: Thailand 1997, Argentina 2001, Global 2008

Train + benchmark:
  XGBoost vs LightGBM vs Logistic Regression vs Always-0 baseline
  Walk-forward CV (không random split!)
  SHAP values + Calibration curve

File: ai/classifier/trainer.py
```

#### Bước 4 — RAG From Scratch (1 tuần)
```
Mục tiêu: Chatbot trả lời câu hỏi về kinh tế dựa trên WB docs

Không dùng LangChain. Tự viết:
  chunk_document() → embed_batch() → upsert_pgvector()
  hybrid_retrieve() (dense + BM25 + RRF) → cross_encoder_rerank()

File: ai/rag/pipeline_scratch.py
```

#### Bước 5 — AI Agents với LangGraph (2 tuần)
```
Mục tiêu: User hỏi bằng ngôn ngữ tự nhiên → Agent trả lời

Agent 1: Risk Scorer (ML model + rule-based, score 0-100)
Agent 2: SQL Agent (Text-to-SQL với schema validation)
Agent 3: RAG Agent (reuse pipeline bước 4)
Agent 4: Country Research (orchestrate các agent trên)
Supervisor: LangGraph StateGraph route đúng agent theo intent

File: ai/agents/economic_copilot.py
```

#### Bước 6 — MLOps Polish (1 tuần)
```
Mục tiêu: Mọi thứ có monitoring, không chạy "mù"

Model Card YAML cho mỗi model
Champion-Challenger: auto so sánh Prophet vs XGBoost
3 loại Drift: Data Drift (KS-test), Prediction Drift (PSI),
              Concept Drift (ADWIN)
Grafana dashboard

File: ai/mlops/champion_challenger.py, ai/mlops/monitoring.py
```

---

## Nguyên Tắc Xuyên Suốt

| Nguyên tắc | Ví dụ cụ thể |
|---|---|
| **Không dùng AutoML** | Tự viết feature engineering, tự chọn hyperparameter |
| **Không dùng LangChain** | Tự implement chunking, retrieval, reranking |
| **Không dùng FinRobot** | Học kiến trúc, tự code agent từ đầu với LangGraph |
| **Mọi thứ có test** | `tests/unit/` + `tests/integration/` cho mỗi component |
| **Walk-forward CV** | Không bao giờ random split time-series data |
| **Explain được** | SHAP, calibration, Model Card — interviewer hỏi được |

---

## Câu Hỏi Phỏng Vấn → Bạn Trả Lời Từ Code Đã Viết

| Câu hỏi | Bạn trả lời |
|---|---|
| *"Chunking strategy của bạn là gì?"* | Sentence-boundary aware, overlap 50 tokens, min 100 tokens |
| *"Tại sao giữ reranker dù latency cao?"* | Ablation: bỏ reranker → faithfulness giảm 14pp |
| *"LoRA là gì?"* | Train A∈ℝ^{m×r} và B∈ℝ^{r×n}, r=16, chỉ 0.034% params |
| *"Calibration là gì?"* | Brier score giảm từ 0.12 → 0.07 sau isotonic regression |
| *"Supervisor handle conflict thế nào?"* | LangGraph state immutable, mỗi node chỉ add không overwrite |

---

## Files Quan Trọng Nhất Để Đọc

| File | Nội dung |
|---|---|
| [GDIP_architecture.md](./GDIP_architecture.md) | Thiết kế toàn bộ hệ thống, code skeleton |
| [GDIP_phases_testing.md](./GDIP_phases_testing.md) | DoR/DoD + test cases cho từng Phase |
| [FAQ_Architecture.md](./FAQ_Architecture.md) | Giải thích ACID, Delta Lake, Postgres vs DuckDB |
