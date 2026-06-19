# GDIP — Kiến Trúc Tổng Thể (Senior Design)
> Global Development Intelligence Platform | v2.0 — Production-Grade Architecture

---

## 1. Nguyên Tắc Thiết Kế (Design Principles)

Trước khi đọc bất kỳ diagram nào, Senior Engineer luôn đặt câu hỏi:
**"Hệ thống này fail như thế nào, và fail đó có chấp nhận được không?"**

| Principle | Áp dụng trong GDIP |
|-----------|-------------------|
| **Idempotency** | Mọi DAG đều có thể re-run mà không duplicate data |
| **Fail-fast** | Data quality gate chặn pipeline sớm, không để bad data vào Gold |
| **Immutability** | Bronze layer không bao giờ bị overwrite — append only |
| **Schema Evolution** | Thêm column mới không break downstream consumers |
| **Observability-first** | Log, metric, trace từ đầu — không phải sau khi có bug |
| **Separation of Concerns** | DE owns pipeline infra; AIE owns model logic; cả hai dùng chung interface contracts |

---

## 2. System Context Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        EXTERNAL SYSTEMS                          │
│  [World Bank API]  [WB PDF Reports]  [Commodity Prices Feed]     │
└────────────┬──────────────┬─────────────────┬────────────────────┘
             │              │                 │
             ▼              ▼                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     GDIP PLATFORM                                │
│                                                                  │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────┐   │
│  │ Ingestion   │───▶│ Storage &    │───▶│ AI Layer           │   │
│  │ Layer       │    │ Transform    │    │ (Forecast/RAG/SQL) │   │
│  └─────────────┘    └──────────────┘    └────────────────────┘   │
│         │                  │                      │              │
│         ▼                  ▼                      ▼              │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │              Orchestration (Airflow)                    │     │
│  │  Schedule · Retry · SLA · Alerting · Dependency Graph  │     │
│  └─────────────────────────────────────────────────────────┘     │
│         │                  │                      │              │
│         ▼                  ▼                      ▼              │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐      │
│  │Observabi-│    │  Data        │    │  Serving Layer     │      │
│  │lity Stack│    │  Governance  │    │  (API/Dashboard)   │      │
│  └──────────┘    └──────────────┘    └────────────────────┘      │
└──────────────────────────────────────────────────────────────────┘
             │                                       │
             ▼                                       ▼
      [Ops/Monitoring]                    [End Users / Consumers]
      Grafana · PagerDuty                Analysts · Policymakers
```

---

## 3. DE Pipeline — Chi Tiết Senior Level

### 3.1 Ingestion Design

#### Idempotency Strategy
Đây là vấn đề quan trọng nhất. World Bank API có thể trả về data đã có — pipeline phải handle được mà không tạo duplicate.

```
Partition Key: country_iso3 + indicator_code + year + fetch_date

Idempotency Pattern:
  1. Mỗi run tạo một "fetch_id" = sha256(country + indicator + date_range + api_version)
  2. Check fetch_id trong Bronze manifest table trước khi fetch
  3. Nếu đã tồn tại → skip (hoặc overwrite nếu force_reload=True)
  4. Sau khi write → update manifest với status=SUCCESS / FAILED

Bronze Manifest Table (PostgreSQL):
  - fetch_id         VARCHAR(64)  PK
  - country_code     CHAR(3)
  - indicator_code   VARCHAR(50)
  - date_range_start DATE
  - date_range_end   DATE
  - fetched_at       TIMESTAMP
  - row_count        INT
  - file_path        TEXT
  - status           ENUM(PENDING, SUCCESS, FAILED, SKIPPED)
  - error_message    TEXT
  - api_version      VARCHAR(10)
```

#### Error Handling & Retry Strategy
```
World Bank API thường fail vì:
  - Rate limiting (429) → Exponential backoff: 1s, 2s, 4s, 8s, max 5 retries
  - Timeout (504)       → Retry với smaller date range (chunk theo 5 năm)
  - Empty response      → Không phải error — log WARNING, mark as EMPTY
  - Schema change       → Detect và alert, không fail silently

Airflow Retry Config (per task):
  retries=3
  retry_delay=timedelta(minutes=5)
  retry_exponential_backoff=True
  max_retry_delay=timedelta(minutes=30)

Circuit Breaker:
  Nếu >10 consecutive failures trong 1 DAG run
  → Halt toàn bộ DAG, send PagerDuty alert
  → Không cố retry vô hạn làm waste API quota
```

#### Incremental Loading Logic
```
Mỗi indicator có update_frequency khác nhau:
  ANNUAL:    fetch year = current_year - 1 (WB thường lag 1 năm)
  QUARTERLY: fetch last 2 quarters
  MONTHLY:   fetch last 3 months

Watermark Table:
  indicator_code | last_fetched_year | last_fetched_at | next_fetch_due
  NY.GDP.MKTP.CD | 2023              | 2024-01-15      | 2025-01-01

High-watermark pattern:
  SELECT MAX(year) FROM bronze WHERE indicator_code = ?
  → Chỉ fetch từ max_year + 1 trở đi
  → Kết hợp với manifest check để tránh duplicate
```

### 3.2 Schema Design — Medallion Architecture

#### Bronze Schema (Raw)
```sql
-- Không enforce schema chặt, schema-on-read với Delta
-- Partition layout: /bronze/indicators/country=VNM/indicator=NY.GDP.MKTP.CD/year=2023/

-- Delta table schema (permissive)
CREATE TABLE bronze.raw_indicators (
    fetch_id          STRING NOT NULL,
    raw_payload       STRING,          -- JSON nguyên gốc từ API
    country_code      STRING,
    indicator_code    STRING,
    year              INT,
    raw_value         STRING,          -- Giữ nguyên string, chưa cast
    fetched_at        TIMESTAMP,
    api_source        STRING,          -- 'WB_API_v2' | 'CSV_BULK'
    _ingested_at      TIMESTAMP DEFAULT current_timestamp(),
    _file_path        STRING            -- cho data lineage
)
USING DELTA
PARTITIONED BY (country_code, year)
TBLPROPERTIES (
    'delta.appendOnly' = 'true',       -- Immutable Bronze
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

#### Silver Schema (Cleaned)
```sql
-- Strongly typed, validated, business-key enforced
CREATE TABLE silver.indicators (
    -- Business Keys
    country_code      CHAR(3)      NOT NULL,  -- ISO 3166-1 alpha-3
    indicator_code    VARCHAR(50)  NOT NULL,  -- WB indicator code
    year              SMALLINT     NOT NULL,

    -- Measured Value
    value             DECIMAL(20,6),
    is_imputed        BOOLEAN      DEFAULT FALSE,  -- linear interpolation flag
    is_outlier        BOOLEAN      DEFAULT FALSE,  -- IQR-based flag
    outlier_score     FLOAT,                       -- Z-score

    -- Metadata
    unit              VARCHAR(50),
    scale             VARCHAR(20),   -- 'millions', 'percent', 'ratio'
    source_note       TEXT,

    -- Lineage
    bronze_fetch_id   STRING       NOT NULL,
    silver_processed_at TIMESTAMP  NOT NULL,
    dbt_run_id        STRING,

    -- Constraints (enforced via dbt tests, not DDL)
    -- PK: (country_code, indicator_code, year)
    -- FK: country_code → dim_country.iso3
    -- FK: indicator_code → dim_indicator.code
    CONSTRAINT pk_silver_indicators
        PRIMARY KEY (country_code, indicator_code, year)
)
USING DELTA
PARTITIONED BY (indicator_code)  -- Query pattern: filter by indicator first
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');  -- For downstream sync
```

#### Gold Schema (Analytics-Ready)
```sql
-- Fact table (wide, denormalized cho BI)
CREATE TABLE gold.fact_indicators (
    country_code      CHAR(3),
    indicator_code    VARCHAR(50),
    year              SMALLINT,
    value             DECIMAL(20,6),

    -- Pre-computed features (tránh compute on-the-fly trong BI)
    value_lag_1y      DECIMAL(20,6),
    value_lag_3y      DECIMAL(20,6),
    value_lag_5y      DECIMAL(20,6),
    yoy_growth_pct    DECIMAL(10,4),   -- (value - lag_1y) / lag_1y * 100
    rolling_avg_3y    DECIMAL(20,6),
    rolling_avg_5y    DECIMAL(20,6),
    rolling_std_5y    DECIMAL(20,6),   -- cho anomaly detection
    z_score_global    DECIMAL(10,4),   -- normalized across all countries

    -- Dimensional denormalization (avoid joins in BI queries)
    country_name      VARCHAR(100),
    region            VARCHAR(50),
    income_group      VARCHAR(30),
    indicator_name    VARCHAR(200),
    indicator_topic   VARCHAR(100),
    unit              VARCHAR(50),

    -- Audit
    _gold_updated_at  TIMESTAMP
)
USING DELTA
PARTITIONED BY (year, indicator_code);

-- Feature Store (cho ML models)
CREATE TABLE gold.feature_store (
    entity_id         STRING,   -- '{country_code}_{indicator_code}'
    feature_date      DATE,
    feature_name      STRING,
    feature_value     DOUBLE,
    feature_version   INT,      -- versioning khi thay đổi feature logic
    _computed_at      TIMESTAMP
)
USING DELTA;
```

### 3.3 Data Quality Gates

```
Layer Transition Rules (không pass gate = pipeline halt):

Bronze → Silver:
  [CRITICAL] raw_payload không phải NULL            → HALT
  [CRITICAL] country_code thuộc ISO 3166-1 alpha-3  → HALT
  [CRITICAL] year trong range [1960, current_year]  → HALT
  [WARNING]  value NULL rate < 30% per indicator    → LOG only
  [WARNING]  Không có duplicate (country+indicator+year) → HALT

Silver → Gold:
  [CRITICAL] Referential integrity dim_country      → HALT
  [CRITICAL] Referential integrity dim_indicator    → HALT
  [CRITICAL] yoy_growth_pct trong [-200%, 200%]    → FLAG (không halt)
  [WARNING]  Row count drop > 5% vs previous run   → ALERT + HALT
  [INFO]     Imputation rate < 10%                 → LOG only

Anomaly Check (Gold):
  [ALERT]    Distribution shift (KS test p < 0.05) → Slack alert
  [ALERT]    New country appears                   → Review queue
```

### 3.4 Airflow DAG Design — Senior Patterns

```python
# Pattern: Sensor → Quality Check → Transform → Validate → Notify

# DAG dependency graph:
#
# wb_ingest_annual
#     └── [BronzeQualityCheck]
#              └── dbt_silver_models
#                       └── [SilverQualityCheck]
#                                └── dbt_gold_models
#                                         └── [GoldQualityCheck]
#                                                  ├── ml_feature_refresh
#                                                  └── superset_cache_refresh

# Cross-DAG dependency (TriggerDagRunOperator):
# wb_ingest_annual ──triggers──▶ dbt_transform
# dbt_transform    ──triggers──▶ ml_retrain (nếu Gold data mới)
# dbt_transform    ──triggers──▶ rag_index  (nếu có PDF mới)

# SLA Config:
# wb_ingest_annual: SLA = 4h (nếu >4h chưa xong → alert)
# dbt_transform:    SLA = 2h
# ml_retrain:       SLA = 6h

# Concurrency Control:
# max_active_runs = 1 (không chạy song song 2 instances cùng DAG)
# max_active_tasks = 4 (parallelism trong 1 DAG run)
```

---

## 4. AI System Design — Chi Tiết Senior Level

### 4.1 Model Versioning Strategy

```
Vấn đề: Có 200+ countries × 10+ indicators = 2000+ models
→ Không thể track thủ công

MLflow Model Registry Architecture:
  Experiment: gdip/forecasting/{indicator_code}
    └── Run: {country_code}_{date}
         ├── Parameters: (seasonality_mode, changepoint_prior, ...)
         ├── Metrics:    (mape_train, mape_holdout, rmse, coverage_90)
         ├── Artifacts:  (model.pkl, feature_importance.json, eval_plots/)
         └── Tags:       (country=VNM, indicator=NY.GDP.MKTP.CD, 
                          data_version=gold_v3, retrain_trigger=scheduled)

Model Lifecycle States:
  None → Staging → Production → Archived

Promotion Rules (automated):
  Staging → Production nếu:
    - mape_holdout < current Production model's mape × 1.05  (không tệ hơn 5%)
    - coverage_90 > 0.88
    - Inference latency p95 < 500ms
    - Không có data leakage (test period phải sau train period)

Rollback Strategy:
  Nếu Production model degraded (drift detected):
    1. Auto-rollback về previous Production version (< 5 phút)
    2. Alert on-call engineer
    3. Flag cho manual review
```

### 4.2 Model Serving Architecture

```
Request Flow:
  Client ──▶ API Gateway (rate limit, auth)
          ──▶ FastAPI App (load balancing)
          ──▶ Model Cache Layer (Redis, TTL=24h)
               ├── Cache HIT  → return cached prediction
               └── Cache MISS → MLflow load model → predict → cache → return

FastAPI Endpoint Design:
  GET /v1/forecast/{country_code}/{indicator_code}
    Params: horizon_years (1-10), confidence_level (0.8|0.9|0.95)
    Response: {
      predictions: [{year, value, lower_bound, upper_bound}],
      model_version: "gdip-prophet-VNM-GDP-v12",
      model_trained_at: "2024-11-01",
      data_as_of: "2023-12-31",
      mape_holdout: 0.087,
      warning: null | "model_stale" | "low_confidence"
    }

  GET /v1/anomaly/{country_code}
    Response: {
      anomalies: [{indicator, year, score, severity, context}],
      scan_timestamp: "...",
      model_version: "..."
    }

Performance Requirements:
  - P50 latency: < 100ms  (cache hit)
  - P95 latency: < 500ms  (cache miss, model inference)
  - P99 latency: < 2000ms
  - Throughput:  > 100 RPS
  - Availability: 99.5%
```

### 4.3 Evaluation Pipeline (Automated)

```
Không thể trust model nếu không có systematic evaluation.
Senior Engineer tách biệt: train eval vs. production eval.

── TRAIN-TIME EVALUATION ──────────────────────────────────────────

Time-series Cross Validation (Walk-forward):
  Không dùng random split! Time-series phải giữ temporal order.
  
  Train: [1960–2010] → Validate: [2011–2013]
  Train: [1960–2013] → Validate: [2014–2016]
  Train: [1960–2016] → Validate: [2017–2019]
  Train: [1960–2019] → Validate: [2020–2022]
  
  Final test: [2023] (holdout, chỉ dùng 1 lần)

Metrics:
  MAPE    = mean(|actual - pred| / actual) × 100   [target: < 15%]
  RMSE    = sqrt(mean((actual - pred)²))
  SMAPE   = symmetric MAPE (xử lý được khi actual ≈ 0)
  Coverage_90 = % actual values nằm trong 90% PI    [target: > 88%]
  Winkler Score = interval sharpness + coverage combined

── PRODUCTION MONITORING ──────────────────────────────────────────

1. Data Drift Detection (chạy daily)
   Input features so sánh: train distribution vs. recent data
   Method: Kolmogorov-Smirnov test (continuous), Chi-squared (categorical)
   Threshold: p-value < 0.05 → DRIFT_ALERT
   Action: Trigger early retrain, alert team

2. Prediction Drift (chạy weekly)
   Compare: prediction distribution vs. historical baseline
   Alert nếu: mean shift > 2σ hoặc variance doubles

3. Ground Truth Validation (chạy khi WB releases new data)
   So sánh model's prediction với actual WB data khi có
   Update model accuracy score trong MLflow
   Nếu MAPE thực tế > 20% → force retrain

4. Model Staleness Check (chạy daily)
   Nếu model chưa retrain > 60 ngày → WARNING
   Nếu model chưa retrain > 90 ngày → FORCE_RETRAIN

Monitoring Dashboard (Grafana):
  - MAPE per country per indicator (heatmap)
  - Drift scores over time (line chart)
  - Model age distribution (histogram)
  - Prediction vs. actual scatter (khi có ground truth)
  - Alert history log
```

### 4.4 RAG System Design — Production Grade

```
── INDEXING PIPELINE ──────────────────────────────────────────────

PDF Processing Strategy:
  Không phải mọi PDF đều xử lý giống nhau.
  
  Document Classification:
    REPORT    → chunk theo section (H1/H2 boundaries)
    DATA_TABLE → extract as structured JSON, không chunk
    POLICY    → chunk theo paragraph, overlap lớn hơn (25%)
    APPENDIX  → skip (noise)

Chunking Decision Matrix:
  Chunk size 512 tokens, overlap 50:
    → Phù hợp factual Q&A
    → Không tốt cho synthesis questions
  
  Chunk size 256 tokens, overlap 80:
    → Precision cao hơn
    → Recall thấp hơn
    
  GDIP chọn: Adaptive chunking
    - Lý do đơn giản: split theo section boundaries trước
    - Nếu section > 600 tokens → sub-chunk theo câu
    - Nếu section < 100 tokens → merge với section kế tiếp

Embedding Strategy:
  Model: text-embedding-3-small (OpenAI) — cost-effective
  Fallback: bge-m3 (local) — nếu cần offline / cost control
  
  Metadata stored per chunk:
    {
      chunk_id, doc_id, source_url,
      page_number, section_title,
      doc_type, publication_year, country_focus,
      token_count, embedding_model_version
    }
  → Dùng metadata filter TRƯỚC vector search để narrow search space

── RETRIEVAL PIPELINE ─────────────────────────────────────────────

Hybrid Search (vector + keyword):
  1. Dense vector search   → top-20 candidates (cosine similarity)
  2. BM25 keyword search   → top-20 candidates
  3. Reciprocal Rank Fusion → merge + re-score → top-10
  4. Cross-encoder reranker → final top-3 (most expensive, done last)

Query Preprocessing:
  Vietnamese → detect language → translate to English for embedding
  (WB docs mostly English, embedding space English-dominant)
  HyDE: Generate hypothetical answer → embed → search
  (Khi câu hỏi quá ngắn hoặc abstract)

Context Assembly:
  Không chỉ lấy top-3 chunks mà còn:
  - Prepend section header của mỗi chunk
  - Append source citation
  - Check total context < model's context window
  - Nếu overflow → summarize lower-ranked chunks

── EVALUATION (RAGAS) ─────────────────────────────────────────────

Eval Dataset Construction:
  50 QA pairs được tạo bằng cách:
  - 20 pairs: manually crafted (ground truth chắc chắn)
  - 20 pairs: LLM-generated từ documents + human review
  - 10 pairs: adversarial (câu hỏi không có answer trong corpus)
  
  Adversarial cases quan trọng để test:
  - Hallucination rate (model bịa khi không biết)
  - Abstention behavior ("I don't have information about...")

Metrics:
  Faithfulness:       Claim trong answer có support từ context không?
  Answer Relevancy:   Answer có trả lời đúng câu hỏi không?
  Context Recall:     Ground truth facts có được retrieve không?
  Context Precision:  Retrieved chunks có relevant không (không noise)?
  Hallucination Rate: % claims không có trong retrieved context [target: < 5%]
```

---

## 5. Interface Contracts (DE ↔ AIE Boundary)

```
Senior insight: DE và AIE phải agree on interface contracts.
AI models KHÔNG được query Bronze/Silver trực tiếp.
AI models chỉ đọc từ Gold layer qua defined interfaces.

Contract 1: Feature Store Interface
  Table: gold.feature_store
  Schema version: v3 (semantic versioning)
  SLA: data available by 08:00 UTC sau mỗi DAG run
  Breaking change protocol: 2-week notice + versioned table

Contract 2: Model Input Spec
  Mỗi model có input_schema.json:
  {
    "model": "gdip-forecast-v2",
    "features": ["value_lag_1y", "value_lag_3y", "rolling_avg_5y"],
    "entity_key": "country_code + indicator_code",
    "temporal_key": "year",
    "min_history_required": 10  // năm
  }
  → Nếu entity có < 10 năm data → model trả về null + warning

Contract 3: API Response Schema
  Versioned: /v1/, /v2/
  Deprecation policy: v(n-1) supported for 6 months sau v(n) release
  Breaking changes require major version bump
```

---

## 6. Failure Modes & Mitigation

| Failure | Probability | Impact | Mitigation |
|---------|-------------|--------|------------|
| WB API down | Medium | High | Fallback to cached Bronze data, retry queue |
| Bad data từ API | Low | High | Bronze quality check + manifest skip |
| Model retrain fails | Low | Medium | Keep current Production model, alert |
| Vector DB slow | Low | Medium | Query timeout + fallback to keyword-only |
| LLM API down | Medium | High | Fallback model (smaller local), graceful degradation |
| Gold data stale | Low | High | SLA alert nếu Gold không update trong 26h |
| Disk full (MinIO) | Low | Critical | Alert at 80% capacity, auto-cleanup Bronze > 3 years |

---

## 7. Observability Stack

```
Three Pillars of Observability:

METRICS (Grafana + Prometheus):
  Pipeline: dag_duration_seconds, task_failure_rate, row_count_delta
  Models:   prediction_latency_p95, mape_rolling_7d, drift_score
  API:      request_rate, error_rate_4xx, error_rate_5xx, cache_hit_ratio
  RAG:      query_latency, retrieval_score_avg, hallucination_rate

LOGS (structured JSON, shipped to Elasticsearch):
  Every pipeline run: {dag_id, run_id, task_id, status, duration, row_count}
  Every model call:   {model_version, entity, latency, cache_hit}
  Every RAG query:    {query_hash, retrieved_docs, response_latency, user_feedback}
  
  Log Levels:
    ERROR   → PagerDuty alert (immediate)
    WARNING → Slack #gdip-alerts (batched every 15 min)
    INFO    → Elasticsearch (7-day retention)
    DEBUG   → Local only (never production)

TRACES (OpenTelemetry):
  End-to-end request tracing: User query → RAG retrieve → LLM → Response
  Airflow task traces: ingest → quality check → transform
  Bottleneck identification: where does latency come from?

DATA LINEAGE (OpenLineage + Marquez):
  Biết chính xác: "Row này trong Gold đến từ Bronze fetch nào?"
  Impact analysis: "Nếu Bronze file X bị corrupt, Gold tables nào bị ảnh hưởng?"
```

---

## 8. ML Classifier — Country Economic Health (Tháng 1 Deep Work)

### 8.1 Bài Toán & Lý Do Chọn

```
Không dùng AutoML hay sklearn pipeline mù quáng.
Mục tiêu: demonstrate ML Engineer mindset — tự define bài toán, tự label data,
justify feature engineering, interpret model.

Đây là thứ Data Scientist và ML Engineer thực sự làm hàng ngày.
```

### 8.2 Feature Engineering (Tự Tay, Có Lý Do)

```python
# ai/classifier/feature_engineering.py

class EconomicFeatureEngineer:
    """
    Feature engineering có chủ đích — mỗi feature đều justify được.

    Design decisions:
    - Lag features: capture delayed effects (policy → GDP thường lag 1-2 năm)
    - Ratio features: normalize absolute values (debt/GDP meaningful hơn debt alone)
    - Momentum: acceleration quan trọng hơn level (đang xấu đi nhanh vs. xấu nhưng ổn định)
    - Cross-country z-score: so sánh được giữa countries có scale khác nhau
    """

    def engineer(self, df: pd.DataFrame) -> pd.DataFrame:
        g = df.sort_values(['country_code', 'year']).groupby('country_code')

        # ── Lag Features ─────────────────────────────────────────────
        df['gdp_growth_lag1']  = g['gdp_growth'].shift(1)   # điều kiện năm trước
        df['gdp_growth_lag3']  = g['gdp_growth'].shift(3)   # trend 3 năm trước
        df['inflation_lag1']   = g['inflation'].shift(1)

        # ── Ratio Features ───────────────────────────────────────────
        df['debt_to_gdp']      = df['govt_debt'] / df['gdp_current'].replace(0, np.nan)
        df['trade_to_gdp']     = df['trade_volume'] / df['gdp_current'].replace(0, np.nan)
        df['reserves_months']  = df['reserves'] / (df['imports'] / 12).replace(0, np.nan)

        # ── Momentum Features (YoY change + acceleration) ─────────────
        df['gdp_yoy']          = g['gdp_growth'].diff(1)    # Δ năm này vs. năm trước
        df['gdp_accel']        = g['gdp_yoy'].diff(1)       # 2nd derivative — tốc độ thay đổi
        df['inflation_accel']  = g['inflation'].diff(1).diff(1)

        # ── Rolling Statistics ────────────────────────────────────────
        df['gdp_rolling3_std'] = g['gdp_growth'].transform(
            lambda x: x.rolling(3, min_periods=2).std()
        )  # volatility signal

        # ── Cross-Country Z-Score Normalization ───────────────────────
        # Tại sao: GDP 5% ở Zimbabwe ≠ GDP 5% ở Germany (context matters)
        for col in ['gdp_growth', 'inflation', 'debt_to_gdp']:
            year_stats = df.groupby('year')[col].agg(['mean', 'std'])
            df = df.join(year_stats.rename(columns={'mean': f'{col}_yr_mean',
                                                     'std':  f'{col}_yr_std'}), on='year')
            df[f'{col}_zscore'] = (df[col] - df[f'{col}_yr_mean']) / \
                                   df[f'{col}_yr_std'].replace(0, np.nan)

        return df.drop(columns=[c for c in df.columns if c.endswith('_yr_mean')
                                                       or c.endswith('_yr_std')])
```

### 8.3 Weak Supervision Labeling

```python
# ai/classifier/labeling.py

def label_economic_health(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tự label data dùng weak supervision — không có "ground truth" sẵn.

    Crisis definition (multi-condition):
      GDP growth < -2%  → recession signal
      Inflation > 20%   → hyperinflation signal
      Currency crash > 30% depreciation in 1 year

    Sau đó validate bằng historical events:
      2008: phải detect ở US, UK, EU
      1997: phải detect ở THA, IDN, MYS, KOR
      2020: phải detect gần như mọi nơi
    """

    df['is_crisis'] = (
        (df['gdp_growth']  < -2.0) |
        (df['inflation']   > 20.0) |
        (df['fx_deprec']  > 30.0)
    ).astype(int)

    # Validate labels bằng known historical events
    KNOWN_CRISES = {
        ('THA', 1997): 1, ('IDN', 1997): 1, ('ARG', 2001): 1,
        ('USA', 2008): 1, ('GRC', 2010): 1, ('VEN', 2016): 1,
    }
    for (country, year), label in KNOWN_CRISES.items():
        mask = (df['country_code'] == country) & (df['year'] == year)
        if df.loc[mask, 'is_crisis'].values[0] != label:
            logger.warning(f"Label mismatch for known crisis: {country} {year}")

    # Log class balance
    balance = df['is_crisis'].value_counts(normalize=True)
    logger.info("label_distribution", crisis_rate=balance.get(1, 0))
    # Nếu < 5% crisis → imbalanced → cần class_weight hoặc SMOTE

    return df
```

### 8.4 Model Training & Evaluation với SHAP + Calibration

```python
# ai/classifier/trainer.py

class EconomicClassifierTrainer:

    def train_and_compare(self, df: pd.DataFrame) -> BenchmarkTable:
        """
        Train nhiều models, so sánh có benchmark table.
        Walk-forward CV — không dùng random split (time-series!).
        """
        models = {
            'XGBoost':           XGBClassifier(scale_pos_weight=20, eval_metric='auc'),
            'LightGBM':          LGBMClassifier(class_weight='balanced', verbose=-1),
            'Logistic_Reg':      LogisticRegression(class_weight='balanced', max_iter=500),
            # Baseline để beat
            'Always_0_baseline': DummyClassifier(strategy='constant', constant=0),
        }

        folds = self._walk_forward_folds(df, n_folds=4, val_years=3)
        results = {}

        for name, model in models.items():
            fold_metrics = []
            for fold in folds:
                X_train, y_train = fold.train_X, fold.train_y
                X_val,   y_val   = fold.val_X,   fold.val_y

                model.fit(X_train, y_train)
                proba = model.predict_proba(X_val)[:, 1]
                pred  = (proba >= 0.5).astype(int)

                fold_metrics.append({
                    'roc_auc':   roc_auc_score(y_val, proba),
                    'f1':        f1_score(y_val, pred),
                    'precision': precision_score(y_val, pred, zero_division=0),
                    'recall':    recall_score(y_val, pred),
                })

            results[name] = {k: np.mean([m[k] for m in fold_metrics])
                             for k in fold_metrics[0]}

        return BenchmarkTable(results)  # pandas DataFrame, printable

    def explain_with_shap(self, model, X: pd.DataFrame) -> SHAPReport:
        """
        SHAP values: feature nào quan trọng nhất?
        "Tại sao model predict nước này là crisis?"
        """
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)

        # Global importance (mean |SHAP|)
        importance = pd.DataFrame({
            'feature': X.columns,
            'importance': np.abs(shap_values).mean(0)
        }).sort_values('importance', ascending=False)

        # Expected answer khi interview:
        # "debt_to_gdp và gdp_growth_lag1 là top 2 features —
        #  điều này make sense vì debt trap thường là lagging indicator của crisis."

        return SHAPReport(
            global_importance=importance,
            shap_values=shap_values,
            summary_plot=shap.summary_plot(shap_values, X, show=False)
        )

    def calibration_analysis(self, model, X_val, y_val) -> CalibrationReport:
        """
        Calibration curve: predicted 70% crisis probability = 70% actual crisis?
        Uncalibrated model → không dùng được cho risk management.
        """
        proba = model.predict_proba(X_val)[:, 1]

        # Isotonic regression calibration
        calibrated_model = CalibratedClassifierCV(model, method='isotonic', cv='prefit')
        calibrated_model.fit(X_val, y_val)
        proba_calibrated = calibrated_model.predict_proba(X_val)[:, 1]

        fraction_pos, mean_pred = calibration_curve(y_val, proba, n_bins=10)
        fraction_pos_cal, mean_pred_cal = calibration_curve(y_val, proba_calibrated, n_bins=10)

        brier_raw = brier_score_loss(y_val, proba)
        brier_cal = brier_score_loss(y_val, proba_calibrated)

        return CalibrationReport(
            raw_brier=brier_raw,
            calibrated_brier=brier_cal,
            improvement=f"{(brier_raw - brier_cal) / brier_raw * 100:.1f}%"
        )
```

---

## 9. RAG From Scratch — Không Dùng LangChain (Tháng 2)

### 9.1 Lý Do Rebuild

```
LangChain ẩn đi tất cả những thứ interviewer muốn bạn hiểu.
Khi bị hỏi "chunking strategy của bạn là gì?" hay "RRF hoạt động thế nào?"
→ bạn trả lời từ CODE đã viết, không phải từ docs đọc qua.
```

### 9.2 Pipeline Từng Bước

```python
# ai/rag/pipeline_scratch.py — không import langchain

# ── BƯỚC 1: Chunking tự implement ────────────────────────────────────
def chunk_document(text: str, max_tokens: int = 512, overlap: int = 50) -> list[str]:
    """
    Không dùng text_splitter của LangChain.
    Tự handle: sentence boundaries, section headers, min chunk size.
    """
    sentences = sent_tokenize(text)   # NLTK, không phải LC
    chunks, current, current_tokens = [], [], 0

    for sent in sentences:
        tok_count = len(enc.encode(sent))   # tiktoken

        if current_tokens + tok_count > max_tokens:
            if current:
                chunks.append(' '.join(current))
                # Overlap: giữ lại sentences cuối
                overlap_sents = []
                overlap_tok   = 0
                for s in reversed(current):
                    if overlap_tok + len(enc.encode(s)) <= overlap:
                        overlap_sents.insert(0, s)
                        overlap_tok += len(enc.encode(s))
                    else:
                        break
                current, current_tokens = overlap_sents, overlap_tok

        current.append(sent)
        current_tokens += tok_count

    if current:
        chunks.append(' '.join(current))

    return chunks


# ── BƯỚC 2: Embedding tự gọi ─────────────────────────────────────────
def embed_batch(texts: list[str], batch_size: int = 100) -> list[list[float]]:
    """Gọi thẳng OpenAI API, không qua LC abstraction."""
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        response = openai.embeddings.create(
            model="text-embedding-3-small",
            input=batch
        )
        all_embeddings.extend([e.embedding for e in response.data])
    return all_embeddings


# ── BƯỚC 3: Vector store tự manage ───────────────────────────────────
def upsert_to_pgvector(chunks: list[str], embeddings: list[list[float]],
                        metadata: list[dict], conn) -> None:
    """Raw SQL, không qua LC abstraction. Biết chính xác SQL đang chạy."""
    with conn.cursor() as cur:
        for chunk, emb, meta in zip(chunks, embeddings, metadata):
            cur.execute("""
                INSERT INTO rag_chunks (chunk_id, content, embedding, metadata, created_at)
                VALUES (%s, %s, %s::vector, %s, NOW())
                ON CONFLICT (chunk_id) DO UPDATE
                SET content   = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    metadata  = EXCLUDED.metadata
            """, (meta['chunk_id'], chunk, emb, json.dumps(meta)))
    conn.commit()


# ── BƯỚC 4: Retrieval — Hybrid với RRF ───────────────────────────────
def hybrid_retrieve(query: str, k: int = 20, conn=None) -> list[RetrievedDoc]:
    """
    Reciprocal Rank Fusion — interviewer hay hỏi cái này.
    Score = Σ 1/(k + rank_i), k=60 là hyperparameter phổ biến.
    """
    query_emb = embed_batch([query])[0]

    # Dense: vector cosine search
    with conn.cursor() as cur:
        cur.execute("""
            SELECT chunk_id, content, metadata,
                   1 - (embedding <=> %s::vector) AS cosine_sim
            FROM rag_chunks
            ORDER BY cosine_sim DESC
            LIMIT %s
        """, (query_emb, k))
        dense_results = cur.fetchall()

    # Sparse: BM25 keyword search (tự implement với PostgreSQL FTS)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT chunk_id, content, metadata,
                   ts_rank_cd(to_tsvector('english', content),
                              plainto_tsquery('english', %s)) AS bm25_score
            FROM rag_chunks
            WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s)
            ORDER BY bm25_score DESC
            LIMIT %s
        """, (query, query, k))
        sparse_results = cur.fetchall()

    # RRF merge
    RRF_K = 60
    scores: dict[str, float] = {}
    for rank, row in enumerate(dense_results):
        scores[row[0]] = scores.get(row[0], 0) + 1.0 / (RRF_K + rank + 1)
    for rank, row in enumerate(sparse_results):
        scores[row[0]] = scores.get(row[0], 0) + 1.0 / (RRF_K + rank + 1)

    # Sort by RRF score, top-k
    top_ids = sorted(scores, key=scores.get, reverse=True)[:k]
    return [r for r in dense_results + sparse_results if r[0] in top_ids]


# ── BƯỚC 5: Cross-encoder Reranker ───────────────────────────────────
def rerank(query: str, candidates: list[RetrievedDoc], top_n: int = 3) -> list[RetrievedDoc]:
    """
    Cross-encoder: score query-document pair jointly (bidirectional attention).
    Tốt hơn bi-encoder vì model thấy cả query và doc cùng lúc.
    Đắt hơn → chỉ dùng cho top-20, không phải toàn bộ corpus.
    """
    from sentence_transformers import CrossEncoder
    reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

    pairs    = [(query, doc.content) for doc in candidates]
    scores   = reranker.predict(pairs)
    ranked   = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)

    return [doc for doc, _ in ranked[:top_n]]
```

---

## 10. Fine-Tuning Small Model — QLoRA (Tháng 2)

### 10.1 Setup & Lý Do

```
Không cần GPU mạnh — Phi-3-mini hoặc Qwen2-1.5B với QLoRA trên Google Colab.
Mục tiêu không phải accuracy — mục tiêu là biết LoRA, quantization,
training loop là gì khi bị hỏi trong interview.
```

### 10.2 Training Pipeline

```python
# ai/finetuning/qlora_train.py (chạy trên Colab T4)

from transformers import AutoModelForSequenceClassification, BitsAndBytesConfig
from peft import get_peft_model, LoraConfig, TaskType

# Task: classify economic indicator descriptions → topic categories
# (ví dụ: "GDP per capita" → "Income/Poverty", "CO2 emissions" → "Environment")

# ── Quantization (4-bit NF4) ──────────────────────────────────────────
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_compute_dtype=torch.float16,
    bnb_4bit_quant_type="nf4",           # NormalFloat4 — tốt hơn INT4 cho LLM weights
    bnb_4bit_use_double_quant=True,      # double quantization tiết kiệm thêm ~0.4 bits/param
)

model = AutoModelForSequenceClassification.from_pretrained(
    "microsoft/Phi-3-mini-4k-instruct",
    quantization_config=bnb_config,
    num_labels=len(TOPIC_CATEGORIES),
    device_map="auto",
)

# ── LoRA Config ───────────────────────────────────────────────────────
lora_config = LoraConfig(
    task_type=TaskType.SEQ_CLS,
    r=16,                    # rank — số lượng trainable parameters
    lora_alpha=32,           # scaling factor (thường = 2×r)
    lora_dropout=0.1,
    # Chỉ train attention layers (không train toàn bộ model)
    target_modules=["q_proj", "v_proj"],
    bias="none",
)

model = get_peft_model(model, lora_config)
model.print_trainable_parameters()
# >> trainable params: 1,310,720 || all params: 3,821,079,552 || trainable%: 0.034%
# → Chỉ train 0.034% params, nhưng domain adaptation đáng kể

# ── Training Loop với Custom Metrics ─────────────────────────────────
trainer = Trainer(
    model=model,
    args=TrainingArguments(
        output_dir="./phi3-wb-classifier",
        num_train_epochs=3,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=4,   # effective batch = 32
        warmup_ratio=0.1,
        learning_rate=2e-4,
        fp16=True,
        logging_steps=10,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1_macro",
    ),
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    compute_metrics=compute_metrics,     # accuracy + f1_macro
)

trainer.train()

# ── Qualitative Error Analysis ────────────────────────────────────────
def analyze_errors(model, test_dataset):
    """
    Không chỉ nhìn accuracy. Nhìn LOẠI lỗi model hay gặp.
    "Model confuse Economic Policy với Trade — vì cả 2 thường co-occur
     trong WB indicator descriptions"
    """
    preds, labels = [], []
    for batch in test_dataset:
        with torch.no_grad():
            output = model(**batch)
        preds.extend(output.logits.argmax(-1).tolist())
        labels.extend(batch['labels'].tolist())

    cm = confusion_matrix(labels, preds)
    errors = [(TOPIC_CATEGORIES[t], TOPIC_CATEGORIES[p], count)
              for t, p_row in enumerate(cm)
              for p, count in enumerate(p_row)
              if t != p and count > 0]
    errors.sort(key=lambda x: x[2], reverse=True)

    return errors[:10]  # Top 10 confusion pairs → qualitative insight
```

---

## 11. Systematic Evaluation & Ablation Study (Tháng 3)

### 11.1 Framework Tổng Quát

```
Đây là thứ phân biệt junior và mid-level AI Engineer rõ nhất.
Junior: "Model accuracy là 87%."
Mid-level: "Accuracy là 87% trên walk-forward CV, 83% trên holdout.
            Diebold-Mariano test cho thấy improvement vs. baseline significant (p<0.01).
            Model fail chủ yếu ở Sub-Saharan Africa — thiếu data trước 1990.
            Ablation: remove SHAP-selected features → accuracy giảm 4.2%."
```

### 11.2 Diebold-Mariano Test (Forecasting)

```python
# ai/evaluation/statistical_tests.py

def diebold_mariano_test(
    actual: np.ndarray,
    pred1:  np.ndarray,
    pred2:  np.ndarray,
    h: int = 1          # forecast horizon
) -> DMTestResult:
    """
    So sánh 2 models có ý nghĩa thống kê không?
    H0: E[d_t] = 0, tức là 2 models không khác nhau.
    Reject H0 (p < 0.05) → Model 1 significantly better/worse than Model 2.

    Không chỉ nhìn mean MAPE! 2% improvement có thể là noise.
    """
    e1 = actual - pred1
    e2 = actual - pred2
    d  = e1**2 - e2**2    # loss differential (squared error)

    mean_d = np.mean(d)
    # Newey-West HAC variance estimate (handle autocorrelation in d_t)
    T   = len(d)
    lags = int(np.ceil(T**(1/3)))
    var_d = newey_west_variance(d, lags=lags)

    dm_stat = mean_d / np.sqrt(var_d / T)
    p_value = 2 * (1 - t.cdf(abs(dm_stat), df=T-1))

    return DMTestResult(
        dm_statistic=dm_stat,
        p_value=p_value,
        significant=(p_value < 0.05),
        better_model="model1" if dm_stat < 0 else "model2"
    )
```

### 11.3 Ablation Study — RAG Pipeline

```python
# ai/evaluation/ablation.py

class RAGAblationStudy:
    """
    Ablation: remove từng component, đo impact.
    Đây là thứ impressive nhất trong interview.

    "Tôi remove reranker và RAGAS faithfulness giảm từ 0.85 xuống 0.71 —
     đó là lý do tôi giữ nó dù tốn thêm 300ms latency."
    """

    CONFIGURATIONS = {
        'full_pipeline':       {'reranker': True,  'bm25': True,  'chunk': 'adaptive'},
        'no_reranker':         {'reranker': False, 'bm25': True,  'chunk': 'adaptive'},
        'no_bm25':             {'reranker': True,  'bm25': False, 'chunk': 'adaptive'},
        'fixed_chunk_512':     {'reranker': True,  'bm25': True,  'chunk': 'fixed_512'},
        'fixed_chunk_256':     {'reranker': True,  'bm25': True,  'chunk': 'fixed_256'},
        'vector_only_baseline':{'reranker': False, 'bm25': False, 'chunk': 'fixed_512'},
    }

    def run(self, eval_dataset: list[QAPair]) -> AblationReport:
        results = {}

        for config_name, config in self.CONFIGURATIONS.items():
            pipeline = self._build_pipeline(**config)
            ragas_scores = []

            for qa in eval_dataset:
                result = pipeline.query(qa.question)
                ragas_scores.append(self._compute_ragas(qa, result))

            results[config_name] = {
                'faithfulness':     np.mean([s.faithfulness for s in ragas_scores]),
                'answer_relevancy': np.mean([s.relevancy    for s in ragas_scores]),
                'context_recall':   np.mean([s.recall       for s in ragas_scores]),
                'avg_latency_ms':   self._measure_latency(pipeline, eval_dataset),
            }

        # Delta vs. full pipeline (để hiểu contribution của từng component)
        baseline = results['full_pipeline']
        for name, metrics in results.items():
            if name != 'full_pipeline':
                results[name]['faithfulness_delta'] = \
                    metrics['faithfulness'] - baseline['faithfulness']

        return AblationReport(configurations=results)

    def document_findings(self) -> str:
        """
        Template cho write-up trong README / interview:
        "Ablation study cho thấy:
         - Reranker: +14pp faithfulness, +300ms latency → KEEP
         - BM25:     +6pp context_recall, +50ms latency → KEEP
         - Adaptive chunk vs. fixed-512: +4pp faithfulness, same latency → KEEP"
        """
        ...
```

### 11.4 Error Analysis Template

```python
# ai/evaluation/error_analysis.py

class ModelErrorAnalyzer:
    """
    Không chỉ nhìn global metrics.
    Nhìn WHERE model fail và WHY — đây là thứ senior engineer làm.
    """

    def analyze_classifier_failures(
        self,
        model,
        df: pd.DataFrame,
        y_pred: np.ndarray
    ) -> ErrorReport:

        df['predicted'] = y_pred
        df['correct']   = (df['is_crisis'] == df['predicted'])

        failures = df[~df['correct']].copy()

        # Phân tích theo dimension
        by_region = failures.groupby('region').size().sort_values(ascending=False)
        by_income = failures.groupby('income_group').size().sort_values(ascending=False)
        by_decade = failures.assign(decade=lambda x: (x['year']//10)*10) \
                            .groupby('decade').size()

        # False Positive analysis: model báo crisis nhưng thực tế không phải
        fp = failures[failures['predicted'] == 1]
        fn = failures[failures['predicted'] == 0]

        # Feature distribution trong FP vs. TP (để understand WHY model bị confuse)
        feature_stats = pd.DataFrame({
            'FP_mean': fp[FEATURE_COLS].mean(),
            'TP_mean': df[df['correct'] & (df['is_crisis'] == 1)][FEATURE_COLS].mean(),
        })

        return ErrorReport(
            by_region=by_region,
            by_income=by_income,
            by_decade=by_decade,
            false_positives=fp,
            false_negatives=fn,
            feature_stats=feature_stats,
            # Expected insight:
            # "Model hay FP ở Sub-Saharan Africa — vì inflation cao nhưng GDP stable
            #  (commodity export economy) → debt_to_gdp feature không phân biệt được"
        )
```

### 11.5 Story Sau 3 Tháng

```
Before:
  "Tôi build pipeline Airflow + dbt với RAG chatbot dùng LangChain."

After:
  "Tôi build end-to-end AI system trên WB data.

   DE layer: Idempotent ingestion với Bronze manifest, dbt medallion,
             Great Expectations quality gates — data quality đảm bảo
             cho cả BI lẫn AI consumption.

   ML layer: Supervised economic health classifier với hand-crafted
             feature engineering (lag, ratio, momentum, cross-country z-score),
             weak supervision labeling validated bằng historical crises,
             XGBoost + LightGBM + Logistic Regression benchmark,
             SHAP interpretability, calibration curve.

   LLM layer: RAG from scratch — không dùng LangChain. Tự implement
              chunking (sentence-boundary aware), embedding, pgvector upsert,
              hybrid retrieval (dense + BM25 + RRF), cross-encoder reranking.
              Fine-tune Phi-3-mini với QLoRA cho WB domain classification.

   Evaluation: Mọi component đều có ablation study documented.
               Diebold-Mariano test cho forecasting.
               RAGAS + LLM-as-Judge cho RAG.
               Calibration plot + threshold analysis cho classifier.
               Error analysis theo region, income group, decade."
```

---

## SECTION 12 — AI Agent Layer (Phase 3)

> Mục tiêu: Biến GDIP từ "Dashboard + Forecast" thành "AI Ra Quyết Định". Đây là phần phân biệt CV AI Engineer với CV Data Engineer thuần túy.

### 12.1 Kiến trúc Tổng Quan

```
User Query
     │
     ▼
Supervisor Agent (LangGraph StateGraph)
     │
     ├──────────────────────────────────┐
     ▼                                  ▼                      ▼
SQL Agent                     Forecast Agent             RAG Agent
(Text-to-SQL)                 (Prophet/XGBoost)          (pgvector)
     │                                  │                      │
     └──────────────────────────────────┘                      │
                         │                                     │
                         ▼                                     │
                 Risk Scoring Agent ◄──────────────────────────┘
                         │
                         ▼
               Country Research Report
```

**Quan điểm thiết kế:** Không dùng FinRobot framework (quá nặng, abstraction cao). Tự build từng agent bằng LangGraph + thuần Python. Đủ nhỏ để hiểu internals, đủ thực để ghi CV.

### 12.2 Economic Copilot Agent

**Use case:** User hỏi câu hỏi tự nhiên về kinh tế, agent tự tìm data và trả lời.

```
User: "Why is Argentina GDP declining?"
          │
          ▼
  Copilot Agent phân tích intent
          │
    ┌─────┴──────┐
    ▼            ▼
SQL Agent    RAG Agent
(query WB    (search WB
 Gold data)   docs/reports)
    │            │
    └─────┬──────┘
          ▼
    Tổng hợp context
          │
          ▼
    Generate insight (OpenAI GPT-4o-mini)
          │
          ▼
  "Argentina GDP fell -2.1% in 2023 due to:
   1. Inflation spike to 211% (highest since 1989)
   2. Currency depreciation 54% vs USD
   3. Drought reducing soy exports 40%..."
```

**Nguyên tắc triển khai:**
- Dùng **LangGraph StateGraph** để orchestrate, không dùng LangChain Agent
- Mỗi Agent là một Python function thuần, có input/output rõ ràng
- State được pass qua các node dưới dạng TypedDict
- Không có "magic" hidden abstraction

```python
# ai/agents/economic_copilot.py

from langgraph.graph import StateGraph, END
from typing import TypedDict

class AgentState(TypedDict):
    query: str
    intent: str                 # "data_query" | "explanation" | "forecast"
    sql_result: dict | None
    rag_context: str | None
    risk_score: float | None
    final_response: str

def route_intent(state: AgentState) -> str:
    """Supervisor: phân tích query, quyết định gọi agent nào."""
    ...

def sql_agent_node(state: AgentState) -> AgentState:
    """Text-to-SQL: convert query thành SQL, chạy trên Gold DB."""
    ...

def rag_agent_node(state: AgentState) -> AgentState:
    """Hybrid search pgvector, rerank, trả về context."""
    ...

def risk_agent_node(state: AgentState) -> AgentState:
    """Tính Risk Score từ ML model + rules."""
    ...

def generate_response_node(state: AgentState) -> AgentState:
    """Gọi LLM tổng hợp tất cả context thành narrative."""
    ...

# Build graph
graph = StateGraph(AgentState)
graph.add_node("route", route_intent)
graph.add_node("sql", sql_agent_node)
graph.add_node("rag", rag_agent_node)
graph.add_node("risk", risk_agent_node)
graph.add_node("generate", generate_response_node)

graph.add_conditional_edges("route", lambda s: s["intent"], {
    "data_query": "sql",
    "explanation": "rag",
    "forecast": "risk",
})
graph.add_edge("sql", "generate")
graph.add_edge("rag", "generate")
graph.add_edge("risk", "generate")
graph.add_edge("generate", END)
```

### 12.3 Risk Scoring Agent

**Mục tiêu:** Kết hợp ML model output + rule-based heuristics → một con số từ 0-100 dễ đọc.

```python
# ai/agents/risk_scorer.py

from dataclasses import dataclass

@dataclass
class RiskComponents:
    inflation_score: float      # 0-25: dựa trên CPI so với threshold
    debt_score: float           # 0-25: Debt/GDP ratio
    unemployment_score: float   # 0-25: unemployment trend
    gdp_decline_score: float    # 0-25: GDP momentum (lag + acceleration)
    ml_classifier_score: float  # Override nếu XGBoost predict crisis > 0.7

def compute_risk_score(country_code: str, year: int) -> RiskComponents:
    """
    Không phải blackbox. Mỗi component có công thức riêng,
    documenting rõ ràng để interviewer có thể hỏi từng phần.

    Ví dụ inflation_score:
        if inflation > 50%  → 25 (hyperinflation)
        if inflation > 20%  → 18
        if inflation > 10%  → 10
        else                → inflation / 10 * 5  (linear)
    """
    ...

def explain_score(components: RiskComponents) -> str:
    """
    Giải thích bằng ngôn ngữ tự nhiên:
    "High risk (score: 78/100). Primary drivers:
     - Debt/GDP at 142% (threshold: 90%) → +25 pts
     - Inflation at 34% → +18 pts
     - ML model probability: 0.81 (crisis) → weight boosted"
    """
    ...
```

### 12.4 Country Research Agent

**Use case:** Generate báo cáo toàn diện cho 1 quốc gia.

```
Input: "Analyze Vietnam economy from 2015-2025"

Output (structured report):
  ┌─────────────────────────────────┐
  │ VIETNAM ECONOMIC REPORT         │
  │ Generated: 2026-06-19           │
  ├─────────────────────────────────┤
  │ 1. Growth Trajectory            │
  │    GDP: 6.8% avg (2015-2019)    │
  │    COVID impact: -2.8% (2021)   │
  │    Recovery: +8.0% (2022)       │
  │                                 │
  │ 2. Risk Assessment              │
  │    Current Score: 28/100 (Low)  │
  │    Inflation: Under control     │
  │    Debt: Manageable             │
  │                                 │
  │ 3. Forecast (12 months)         │
  │    GDP: 6.1-6.5% (Prophet)      │
  │    Confidence: 80%              │
  │                                 │
  │ 4. Key Risks                    │
  │    - External debt rising       │
  │    - Exchange rate pressure     │
  └─────────────────────────────────┘
```

### 12.5 Text-to-SQL Agent

**Quan trọng:** Không dùng LLM generate SQL "tự do" vì dễ bị SQL injection hoặc hallucinate tên cột. Dùng approach "constrained generation":

```python
# ai/agents/text_to_sql.py

ALLOWED_TABLES = ["gold.feature_store", "gold.country_risk", "gold.forecasts"]
SCHEMA_CONTEXT = """
Table: gold.feature_store
Columns: country_code, country_name, year, gdp_growth, inflation,
         fdi_inflow, unemployment, external_debt, total_reserves
"""

def text_to_sql(query: str) -> str:
    """
    1. Inject schema context vào prompt
    2. LLM generate SQL (temperature=0 cho deterministic)
    3. Validate: chỉ cho phép SELECT, không UPDATE/DELETE
    4. Validate: chỉ query trên ALLOWED_TABLES
    5. Thực thi trên read-only DB connection
    """
    prompt = f"""
    Given this database schema:
    {SCHEMA_CONTEXT}

    Convert this question to SQL (SELECT only):
    {query}

    Rules:
    - Only use tables: {ALLOWED_TABLES}
    - No subqueries more than 2 levels deep
    - Always include LIMIT 1000
    """
    ...
```

---

## SECTION 13 — MLOps Layer (Phase 4)

> Mục tiêu: Mọi model đều được track, compare, và deploy có kiểm soát. Không có "tôi train xong rồi copy file pkl vào server".

### 13.1 Model Card (YAML Standard)

Mỗi model lên Production bắt buộc phải có Model Card:

```yaml
# models/cards/gdp_forecast_vnm_v1.2.yaml

model_name: GDP Forecast VNM
version: "1.2"
created_at: "2026-06-19"
owner: GDIP Team
mlflow_run_id: "abc123def456"

algorithm: Prophet
task: time_series_forecasting
target: gdp_growth

training:
  period: "1960-2022"
  n_samples: 63
  features: [gdp_growth, inflation, fdi_inflow]
  cv_strategy: walk_forward_5_folds

performance:
  holdout_period: "2023"
  mape: 8.2
  rmse: 0.94
  diebold_mariano_vs_arima: {p_value: 0.031, better: true}

limitations:
  - "Không reliable cho các quốc gia conflict zone (data gaps > 5 years)"
  - "Accuracy giảm 30% khi có structural break (war, pandemic)"

fairness:
  - "Evaluate separately: Low-income vs High-income countries"
  - "MAPE: 9.1% (low-income) vs 6.8% (high-income)"

deployment:
  endpoint: /v1/forecast/{country_code}
  sla_latency_p95: 800ms
  monitoring: prometheus + grafana
```

### 13.2 Champion-Challenger Framework

```python
# ai/mlops/champion_challenger.py

class ChampionChallengerFramework:
    """
    Không để tay chọn model. Auto-evaluate dựa trên holdout set.

    Challenger: model mới vừa train
    Champion: model đang chạy Production

    Chỉ promote Challenger khi:
    1. MAPE thấp hơn Champion ít nhất 5%
    2. Diebold-Mariano test: p < 0.05
    3. Không có data leakage (walk-forward CV verified)
    4. Latency p95 < 1000ms
    """

    def evaluate_and_promote(
        self,
        challenger_run_id: str,
        champion_run_id: str,
        holdout_df: pd.DataFrame
    ) -> PromotionDecision:

        challenger_mape = self._compute_mape(challenger_run_id, holdout_df)
        champion_mape   = self._compute_mape(champion_run_id, holdout_df)

        improvement_pct = (champion_mape - challenger_mape) / champion_mape

        dm_result = diebold_mariano_test(
            actual=holdout_df['actual'],
            pred1=challenger_preds,
            pred2=champion_preds
        )

        should_promote = (
            improvement_pct >= 0.05 and
            dm_result.p_value < 0.05 and
            self._check_latency(challenger_run_id) < 1000
        )

        if should_promote:
            mlflow.MlflowClient().transition_model_version_stage(
                name="gdp_forecast",
                version=challenger_version,
                stage="Production"
            )

        return PromotionDecision(
            promoted=should_promote,
            reason=self._explain_decision(improvement_pct, dm_result)
        )
```

### 13.3 Feature Store Versioning

```sql
-- gold.feature_store_v2 (thêm metadata)
CREATE TABLE gold.feature_store (
    country_code        VARCHAR(3),
    year                INT,

    -- Core features
    gdp_growth          FLOAT,
    inflation           FLOAT,
    fdi_inflow          FLOAT,
    unemployment        FLOAT,
    external_debt       FLOAT,
    total_reserves      FLOAT,

    -- Engineered features (AI Engineer tạo ra)
    gdp_growth_lag1     FLOAT,
    gdp_growth_lag3     FLOAT,
    inflation_momentum  FLOAT,   -- YoY change in inflation
    debt_to_gdp         FLOAT,
    reserves_to_imports FLOAT,
    gdp_zscore          FLOAT,   -- cross-country normalized

    -- Feature lineage (quan trọng cho MLOps)
    feature_version     VARCHAR(10),   -- "v2.1"
    pipeline_run_id     VARCHAR(64),   -- link to Airflow DAG run
    computed_at         TIMESTAMP,

    PRIMARY KEY (country_code, year, feature_version)
);
```

### 13.4 Online Monitoring (3 loại Drift)

```python
# ai/mlops/monitoring.py

class ProductionMonitor:
    """
    3 loại drift cần monitor — mỗi loại có alert riêng.
    """

    def check_data_drift(self, baseline_df, current_df) -> DriftReport:
        """
        Data Drift: Phân phối input feature thay đổi.
        Ví dụ: Inflation trung bình tăng từ 3% lên 12% post-COVID.
        Dùng: KS-test (continuous) hoặc Chi-square (categorical)
        Alert threshold: p < 0.05 cho ≥ 3 features
        """
        ...

    def check_prediction_drift(self, recent_preds) -> DriftReport:
        """
        Prediction Drift: Model đột ngột predict rất khác thường.
        Ví dụ: Trước đây 95% predict "stable", giờ 60% predict "crisis".
        Dùng: PSI (Population Stability Index) > 0.2 → alert
        """
        ...

    def check_concept_drift(self, recent_errors) -> DriftReport:
        """
        Concept Drift: Relationship giữa features và target thay đổi.
        Ví dụ: Sau 2020, model trained trên pre-COVID data không còn
        đúng nữa vì rules kinh tế thay đổi.
        Dùng: ADWIN algorithm hoặc Page-Hinkley test
        Đây là loại drift khó phát hiện nhất.
        """
        ...

    def export_to_prometheus(self, report: DriftReport):
        """Push metrics lên Prometheus → Grafana dashboard."""
        ...
```

### 13.5 Story Sau Khi Có Phase 3 + Phase 4

```
CV Before (chỉ có DE + ML):
  "Tôi build pipeline WB data, train XGBoost predict crisis."

CV After (Phase 3 + Phase 4):
  "Tôi build end-to-end AI Platform:

   Agent Layer: Multi-agent system dùng LangGraph.
     Supervisor orchestrate 4 specialized agents:
     SQL Agent (Text-to-SQL với schema-constrained generation),
     RAG Agent (hybrid pgvector + BM25, cross-encoder reranking),
     Forecast Agent (Champion-Challenger Prophet vs XGBoost),
     Risk Scoring Agent (ML + explainable rule-based combination).
     Country Research Agent generate structured report tự động.

   MLOps Layer: Mọi model có Model Card YAML (standard).
     Champion-Challenger với Diebold-Mariano statistical test
     để validate improvement trước khi promote lên Production.
     3 loại monitoring: Data Drift (KS-test), Prediction Drift (PSI),
     Concept Drift (ADWIN). Dashboard Prometheus + Grafana."

Interview Q: "Supervisor của bạn handle conflict giữa agents thế nào?"
A: "Trong LangGraph StateGraph, mỗi agent trả về partial state update.
    Supervisor node đọc intent field trong State, route sang đúng agent.
    Nếu cả SQL Agent và RAG Agent đều chạy, generate_response node
    nhận cả hai outputs trong State và LLM tổng hợp. Không có conflict
    vì state là immutable — mỗi node chỉ add, không overwrite."
```
