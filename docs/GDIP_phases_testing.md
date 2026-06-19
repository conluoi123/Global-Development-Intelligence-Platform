# GDIP — Phases & Testing Strategy (Senior Design)
> v2.0 — Production Engineering Mindset

---

## Triết Lý Tổng Quát

Senior Engineer không viết TODO list. Họ viết **Definition of Ready** (trước khi bắt đầu) và **Definition of Done** (trước khi gọi là xong). Mỗi phase ở đây có cả hai.

---

## PHASE 1 — Foundation & DE Pipeline (Tuần 1–4)

### Definition of Ready
- [ ] Docker Desktop cài sẵn, máy có ít nhất 16GB RAM
- [ ] World Bank API key (free) đã test được
- [ ] Git repo + branch strategy đã thống nhất (main/develop/feature/*)
- [ ] `.env.example` đã có, không commit secrets

### Tuần 1 — Infrastructure

**Docker Compose Architecture:**
```yaml
# Không phải chỉ "chạy được" mà phải đúng production patterns

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: gdip_meta      # metadata DB, không phải data DB
    volumes:
      - ./init-scripts:/docker-entrypoint-initdb.d  # auto-create schemas
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5

  airflow-webserver:
    depends_on:
      postgres: { condition: service_healthy }   # không start trước DB sẵn sàng
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__FERNET_KEY: ${FERNET_KEY}   # từ .env, không hardcode
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'       # tắt examples
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/plugins:/opt/airflow/plugins
      - ./airflow/logs:/opt/airflow/logs         # persist logs

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_PASSWORD}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
```

**Folder Structure (enforced từ ngày 1):**
```
gdip/
├── .github/
│   └── workflows/
│       ├── test.yml         # CI tests mỗi PR
│       └── deploy.yml       # CD khi merge vào main
├── airflow/
│   ├── dags/
│   │   ├── ingestion/
│   │   │   ├── wb_ingest_annual.py
│   │   │   └── wb_ingest_monthly.py
│   │   ├── transform/
│   │   │   └── dbt_transform.py
│   │   └── ai/
│   │       ├── ml_retrain.py
│   │       └── rag_index.py
│   └── plugins/
│       ├── operators/
│       │   └── wb_api_operator.py    # custom operator
│       └── hooks/
│           └── minio_hook.py
├── dbt/
│   ├── models/
│   │   ├── bronze/       # staging models
│   │   ├── silver/       # intermediate models
│   │   └── gold/         # mart models
│   ├── tests/
│   │   └── generic/      # custom test macros
│   ├── macros/
│   └── dbt_project.yml
├── ingestion/
│   ├── wb_client.py      # World Bank API wrapper
│   ├── pdf_crawler.py
│   └── manifest.py       # Bronze manifest management
├── ai/
│   ├── forecasting/
│   ├── anomaly/
│   ├── rag/
│   └── agents/
├── api/
│   ├── main.py
│   ├── routers/
│   └── schemas/          # Pydantic models
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── infra/
    └── terraform/
```

### Tuần 2 — Ingestion Layer (Senior Patterns)

**WB API Wrapper với Retry + Circuit Breaker:**
```python
# ingestion/wb_client.py

class WorldBankClient:
    """
    Production-grade WB API client.
    
    Design decisions:
    - Idempotent: cùng params → cùng kết quả, safe to retry
    - Batched: fetch multiple countries/indicators per request
    - Observable: log mọi request với structured JSON
    """
    
    def fetch_indicator(
        self,
        indicator: str,
        countries: list[str],
        date_range: tuple[int, int],
        fetch_id: str,          # caller cung cấp, để idempotency
    ) -> FetchResult:
        
        # 1. Idempotency check
        if self.manifest.exists(fetch_id):
            logger.info("fetch_skipped", fetch_id=fetch_id, reason="already_exists")
            return FetchResult.skipped(fetch_id)
        
        # 2. Chunk date range nếu quá lớn (WB limit 1000 rows/request)
        chunks = self._chunk_date_range(date_range, chunk_size=10)
        
        all_records = []
        for chunk in chunks:
            try:
                records = self._fetch_with_retry(indicator, countries, chunk)
                all_records.extend(records)
            except APIRateLimitError:
                time.sleep(self.backoff.next())   # exponential backoff
                continue
            except APISchemaChangedError:
                self.manifest.mark_failed(fetch_id, "SCHEMA_CHANGED")
                alert.send("WB API schema changed for " + indicator)
                raise   # halt, cần human review
        
        # 3. Write to Bronze (atomic)
        file_path = self._write_to_bronze(all_records, fetch_id)
        self.manifest.mark_success(fetch_id, len(all_records), file_path)
        
        logger.info("fetch_complete",
                    fetch_id=fetch_id,
                    row_count=len(all_records),
                    file_path=file_path)
        return FetchResult.success(fetch_id, all_records)
```

### Tuần 3 — dbt Models (Senior Patterns)

**dbt Silver Model với Quality Assertions:**
```sql
-- models/silver/silver_indicators.sql

{{
  config(
    materialized='incremental',
    unique_key=['country_code', 'indicator_code', 'year'],
    on_schema_change='append_new_columns',   -- schema evolution safe
    incremental_strategy='merge',
    post_hook=[
      "{{ log_model_stats(this) }}",         -- custom macro: log row counts
    ]
  )
}}

WITH bronze_raw AS (
    SELECT * FROM {{ ref('bronze_indicators') }}
    
    {% if is_incremental() %}
    -- Chỉ process records mới hơn last run
    WHERE _ingested_at > (SELECT MAX(silver_processed_at) FROM {{ this }})
    {% endif %}
),

typed_and_validated AS (
    SELECT
        UPPER(TRIM(country_code))::CHAR(3)    AS country_code,
        UPPER(TRIM(indicator_code))           AS indicator_code,
        year::SMALLINT                        AS year,
        
        -- Safe cast với null thay vì exception
        TRY_CAST(raw_value AS DECIMAL(20,6))  AS value,
        
        -- Imputation flag (sẽ được fill ở bước sau)
        (raw_value IS NULL OR raw_value = '')  AS needs_imputation,
        
        -- Outlier detection (Z-score, computed per country×indicator group)
        ABS(
            (TRY_CAST(raw_value AS DECIMAL(20,6)) - AVG(TRY_CAST(raw_value AS DECIMAL(20,6)))
                OVER (PARTITION BY country_code, indicator_code))
            / NULLIF(STDDEV(TRY_CAST(raw_value AS DECIMAL(20,6)))
                OVER (PARTITION BY country_code, indicator_code), 0)
        )                                      AS z_score,
        
        bronze_fetch_id,
        CURRENT_TIMESTAMP()                   AS silver_processed_at,
        '{{ invocation_id }}'                 AS dbt_run_id   -- lineage!
        
    FROM bronze_raw
    WHERE country_code IS NOT NULL
      AND indicator_code IS NOT NULL
      AND year BETWEEN 1960 AND YEAR(CURRENT_DATE()) + 1
)

SELECT
    *,
    (z_score > 3.5)  AS is_outlier,    -- 3.5σ threshold
    FALSE            AS is_imputed     -- imputation chạy ở post-hook
FROM typed_and_validated
```

**dbt Schema Tests:**
```yaml
# models/silver/schema.yml

models:
  - name: silver_indicators
    description: "Cleaned, typed, validated World Bank indicators"
    
    constraints:
      - type: primary_key
        columns: [country_code, indicator_code, year]
    
    columns:
      - name: country_code
        tests:
          - not_null
          - relationships:
              to: ref('dim_country')
              field: iso3
              severity: error   # hard fail, không phải warn
      
      - name: year
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1960
              max_value: "{{ var('current_year') }}"
      
      - name: value
        tests:
          - gdip_not_all_null_per_group:   # custom test macro
              group_by: [country_code, indicator_code]
              max_null_rate: 0.4           # alert nếu >40% null
    
    tests:
      - dbt_utils.expression_is_true:
          expression: "silver_processed_at >= '2020-01-01'"
          name: "processed_date_sanity_check"
      
      - gdip_row_count_vs_previous_run:    # custom test
          max_decrease_pct: 5              # fail nếu row count giảm >5%
```

### Definition of Done — Phase 1
- [ ] `dbt test` pass 100%, zero warnings
- [ ] Great Expectations checkpoint pass tất cả CRITICAL rules
- [ ] Airflow DAG re-run 3 lần liên tiếp → không tạo duplicate (idempotency)
- [ ] Bronze manifest đầy đủ record cho mọi fetch
- [ ] Superset dashboard load trong < 3s
- [ ] README có diagram, setup instructions, và troubleshooting guide
- [ ] Không có secrets trong git history (`git log --all -p | grep -i password` → clean)

---

## PHASE 2 — AI Forecasting & Anomaly (Tuần 5–8)

### Definition of Ready
- [ ] Gold feature_store table có đủ data cho 20+ countries
- [ ] MLflow server đã chạy và accessible
- [ ] Holdout set (2021–2023) đã được lock — không ai chạm vào

### Tuần 5–6 — Forecasting: Walk-forward Validation

**Không dùng random train-test split:**
```python
# ai/forecasting/evaluation.py

class WalkForwardValidator:
    """
    Walk-forward cross-validation cho time-series.
    
    Mỗi fold: train trên [t0, t_train_end], validate trên [t_train_end+1, t_val_end]
    Không bao giờ validate trên data trước train_end (look-ahead bias).
    """
    
    def create_folds(
        self,
        data: pd.DataFrame,
        n_folds: int = 4,
        val_window_years: int = 3,
        min_train_years: int = 20
    ) -> list[Fold]:
        
        years = sorted(data['year'].unique())
        folds = []
        
        for i in range(n_folds):
            val_end   = max(years) - (n_folds - 1 - i) * val_window_years
            val_start = val_end - val_window_years + 1
            train_end = val_start - 1
            
            if train_end - min(years) + 1 < min_train_years:
                continue   # không đủ training data, skip fold
            
            folds.append(Fold(
                train_mask = data['year'] <= train_end,
                val_mask   = (data['year'] >= val_start) & (data['year'] <= val_end),
                fold_id    = i
            ))
        
        return folds
    
    def evaluate(self, model, data, folds) -> EvalResult:
        fold_metrics = []
        
        for fold in folds:
            train = data[fold.train_mask]
            val   = data[fold.val_mask]
            
            model.fit(train)
            predictions = model.predict(val['year'].values)
            
            metrics = {
                'mape':         mape(val['value'], predictions['mean']),
                'rmse':         rmse(val['value'], predictions['mean']),
                'coverage_90':  coverage(val['value'],
                                         predictions['lower_90'],
                                         predictions['upper_90']),
                'fold_id':      fold.fold_id,
                'n_train':      fold.train_mask.sum(),
            }
            fold_metrics.append(metrics)
        
        return EvalResult(
            mean_mape        = np.mean([m['mape'] for m in fold_metrics]),
            std_mape         = np.std([m['mape']  for m in fold_metrics]),
            mean_coverage_90 = np.mean([m['coverage_90'] for m in fold_metrics]),
            fold_details     = fold_metrics,
            # std_mape cao → model không stable → red flag
        )
```

**MLflow Logging Pattern:**
```python
# ai/forecasting/trainer.py

def train_and_register(country: str, indicator: str, data: pd.DataFrame):
    
    with mlflow.start_run(
        run_name=f"{country}_{indicator}_{date.today()}",
        tags={
            "country": country,
            "indicator": indicator,
            "data_version": get_gold_version(),   # track data version!
            "trigger": "scheduled_retrain"
        }
    ) as run:
        
        # 1. Log input data stats (để debug data issues sau này)
        mlflow.log_metrics({
            "train_n_rows":     len(data),
            "train_null_rate":  data['value'].isna().mean(),
            "train_year_min":   data['year'].min(),
            "train_year_max":   data['year'].max(),
        })
        
        # 2. Train
        model = ProphetWrapper(
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05,
            yearly_seasonality=True,
        )
        mlflow.log_params(model.get_params())
        
        validator = WalkForwardValidator()
        eval_result = validator.evaluate(model, data, validator.create_folds(data))
        
        # 3. Log evaluation
        mlflow.log_metrics({
            "mape_cv_mean":     eval_result.mean_mape,
            "mape_cv_std":      eval_result.std_mape,    # stability signal
            "coverage_90_mean": eval_result.mean_coverage_90,
        })
        
        # 4. Train trên full data, log holdout performance
        model.fit(data[data['year'] < HOLDOUT_START])
        holdout_metrics = evaluate_on_holdout(model, holdout_data)
        mlflow.log_metrics({f"holdout_{k}": v for k, v in holdout_metrics.items()})
        
        # 5. Log artifacts
        mlflow.log_figure(plot_forecast(model, data), "forecast_plot.html")
        mlflow.log_dict(eval_result.fold_details, "cv_fold_details.json")
        
        # 6. Register model
        mlflow.prophet.log_model(model, "model",
                                  registered_model_name=f"gdip-forecast-{country}-{indicator}")
        
        # 7. Auto-promote nếu tốt hơn current Production
        maybe_promote_to_production(run.info.run_id, eval_result, holdout_metrics)
```

### Tuần 7 — Anomaly Detection: Calibration

```python
# Senior insight: Anomaly detection không có ground truth rõ ràng.
# Phải dùng historical crises để validate.

KNOWN_CRISES = {
    "2008": {"countries": ["ALL"], "indicators": ["NY.GDP.MKTP.KD.ZG", "FP.CPI.TOTL.ZG"]},
    "2020": {"countries": ["ALL"], "indicators": ["ALL"]},
    "1997": {"countries": ["THA", "IDN", "MYS", "KOR"], "indicators": ["PA.NUS.FCRF"]},
}

class AnomalyCalibrator:
    """
    Calibrate threshold để maximize F1 trên known crises,
    subject to constraint: FPR < 0.05 trên stable periods.
    """
    
    def find_optimal_threshold(self, scores: np.ndarray, labels: np.ndarray):
        # labels: 1 = known crisis year, 0 = stable year
        
        thresholds = np.percentile(scores, np.arange(80, 99, 0.5))
        best = {"threshold": None, "f1": 0, "fpr": 1.0}
        
        for t in thresholds:
            predictions = (scores >= t).astype(int)
            fpr = false_positive_rate(labels, predictions)
            f1  = f1_score(labels, predictions)
            
            if fpr < 0.05 and f1 > best["f1"]:   # hard constraint on FPR
                best = {"threshold": t, "f1": f1, "fpr": fpr}
        
        return best
```

### Tuần 8 — Drift Monitoring Setup

```python
# ai/monitoring/drift_detector.py

class DriftDetector:
    """
    Chạy daily bởi Airflow DAG: anomaly_scan
    Compare feature distribution: training window vs. recent window
    """
    
    def detect_data_drift(
        self,
        reference_data: pd.DataFrame,   # training period data
        current_data: pd.DataFrame,      # last 90 days
        features: list[str]
    ) -> DriftReport:
        
        drift_results = {}
        
        for feature in features:
            ref = reference_data[feature].dropna()
            cur = current_data[feature].dropna()
            
            # KS test: so sánh phân phối
            ks_stat, p_value = ks_2samp(ref, cur)
            
            # PSI: Population Stability Index (industry standard)
            psi = compute_psi(ref, cur, buckets=10)
            
            drift_results[feature] = {
                "ks_statistic":   ks_stat,
                "ks_p_value":     p_value,
                "psi":            psi,
                "drifted":        p_value < 0.05 or psi > 0.2,
                "severity":       "HIGH" if psi > 0.25 else "MEDIUM" if psi > 0.1 else "LOW"
            }
        
        # Aggregate
        n_drifted = sum(v["drifted"] for v in drift_results.values())
        
        report = DriftReport(
            run_date=date.today(),
            feature_results=drift_results,
            overall_drifted=(n_drifted / len(features) > 0.3),  # >30% features drifted
            recommendation="retrain" if n_drifted > 2 else "monitor"
        )
        
        # Log to MLflow as a separate "monitoring" run
        self._log_drift_to_mlflow(report)
        
        if report.overall_drifted:
            alert.send_slack(
                channel="#gdip-alerts",
                message=f"🚨 Data drift detected: {n_drifted}/{len(features)} features drifted",
                details=report.to_dict()
            )
        
        return report
```

### Definition of Done — Phase 2
- [ ] Walk-forward CV implemented, không phải random split
- [ ] MAPE holdout < 15% cho top 10 countries × 3 indicators
- [ ] `std_mape` (cross-fold stability) < 5% — model stable
- [ ] Anomaly detector recall > 80% trên 2008 + 2020 crises
- [ ] FPR < 5% trên stable periods (1995–2006)
- [ ] MLflow experiment đầy đủ: params, metrics, artifacts, tags
- [ ] Drift detector chạy được end-to-end với mock data
- [ ] FastAPI `/predict` endpoint có response schema validation (Pydantic)
- [ ] Load test: 50 concurrent requests, p95 < 500ms

---

## PHASE 3 — NLP, RAG & Agents (Tuần 9–12)

### Tuần 9–10 — RAG với Evaluation-driven Development

**Senior insight: Build evaluation framework TRƯỚC khi build RAG.**
Không thể cải thiện thứ không đo được.

```python
# Bước 1: Tạo eval dataset (trước khi viết RAG code)
# ai/rag/create_eval_dataset.py

EVAL_QUESTIONS = [
    # Factual (should retrieve exactly)
    {
        "question": "What was Vietnam's GDP growth rate in 2022?",
        "ground_truth": "Vietnam's GDP grew by approximately 8.02% in 2022",
        "expected_source": "WB Vietnam Economic Update 2023",
        "category": "factual"
    },
    # Synthesis (requires multiple chunks)
    {
        "question": "How did COVID-19 affect Southeast Asian economies differently?",
        "ground_truth": "...",
        "category": "synthesis"
    },
    # Adversarial (not in corpus — model should abstain)
    {
        "question": "What is the GDP of Mars colony in 2023?",
        "ground_truth": None,
        "category": "adversarial",
        "expected_behavior": "abstain"
    }
]

# Bước 2: Chạy eval sau mỗi thay đổi RAG pipeline
# Treat RAG như code: mỗi change cần pass eval threshold trước khi merge
```

**Chunking Strategy với Tests:**
```python
# ai/rag/chunker.py

class AdaptiveChunker:
    """
    Adaptive chunking: section-aware, không chỉ fixed token count.
    
    Tested với pytest — chunking logic phải deterministic và testable.
    """
    
    def chunk_document(self, doc: ParsedDocument) -> list[Chunk]:
        chunks = []
        
        for section in doc.sections:
            if section.type == "data_table":
                # Tables → structured JSON, không chunk
                chunks.append(Chunk(
                    content=json.dumps(section.to_dict()),
                    chunk_type="table",
                    metadata={**section.metadata, "format": "json"}
                ))
                
            elif section.type == "appendix":
                continue   # skip noise
                
            else:
                # Text sections → adaptive chunking
                section_chunks = self._chunk_text(
                    text=section.content,
                    max_tokens=512,
                    overlap_tokens=50,
                    split_on="sentence",   # không split giữa chừng câu
                    min_chunk_tokens=100,   # merge chunk nhỏ với chunk sau
                )
                
                for i, chunk in enumerate(section_chunks):
                    chunks.append(Chunk(
                        content=chunk,
                        chunk_type="text",
                        metadata={
                            **section.metadata,
                            "chunk_index":    i,
                            "total_chunks":   len(section_chunks),
                            "section_title":  section.title,  # prepend khi retrieve
                        }
                    ))
        
        return chunks
```

### Tuần 11 — Text-to-SQL: Safety & Accuracy

```python
# ai/agents/text_to_sql.py

class SafeTextToSQLAgent:
    """
    Senior concerns:
    1. SQL injection prevention
    2. Query timeout + row limit
    3. Only SELECT allowed
    4. Schema context management (không expose sensitive columns)
    5. Explain SQL trước khi execute (cho transparency)
    """
    
    SAFE_SCHEMA_CONTEXT = """
    Available tables (READ-ONLY, SELECT only):
    
    gold.fact_indicators:
      - country_code CHAR(3)     -- ISO country code (e.g. 'VNM', 'THA')
      - indicator_code VARCHAR   -- WB indicator (e.g. 'NY.GDP.MKTP.CD')
      - year SMALLINT            -- 1960 to present
      - value DECIMAL            -- measured value
      - yoy_growth_pct DECIMAL   -- year-over-year growth %
      - country_name VARCHAR     -- full country name
      - region VARCHAR           -- e.g. 'East Asia & Pacific'
      - indicator_name VARCHAR   -- human-readable indicator name
    
    IMPORTANT: Only generate SELECT queries. No INSERT, UPDATE, DELETE, DROP, CREATE.
    Limit results to 1000 rows maximum.
    """
    
    def execute_query(self, user_question: str, language: str = "vi") -> QueryResult:
        
        # 1. Translate nếu Vietnamese
        if language == "vi":
            english_question = self.translator.to_english(user_question)
        else:
            english_question = user_question
        
        # 2. Generate SQL với schema context
        generated_sql = self.llm.generate(
            system=f"Generate SQL for: {self.SAFE_SCHEMA_CONTEXT}",
            user=english_question,
            few_shot_examples=self.few_shot_store.get_similar(english_question, k=3)
        )
        
        # 3. Validate SQL safety TRƯỚC khi execute
        validation = self.sql_validator.validate(generated_sql)
        if not validation.is_safe:
            return QueryResult.error(f"Unsafe SQL: {validation.reason}")
        
        # 4. Execute với timeout và row limit
        try:
            df = self.db.execute(
                f"SELECT * FROM ({generated_sql}) LIMIT 1000",
                timeout=10   # seconds
            )
        except TimeoutError:
            return QueryResult.error("Query timed out (>10s). Please narrow your query.")
        
        # 5. Generate chart nếu phù hợp
        chart = self.chart_generator.auto_chart(df, user_question)
        
        # 6. Generate natural language summary
        summary = self.llm.summarize(df, user_question, language=language)
        
        return QueryResult(
            sql=generated_sql,
            dataframe=df,
            chart=chart,
            summary=summary,
            row_count=len(df)
        )
```

### Definition of Done — Phase 3
- [ ] RAGAS metrics: faithfulness > 0.85, relevancy > 0.80, recall > 0.75
- [ ] Adversarial questions: abstention rate > 90% (model không bịa)
- [ ] Text-to-SQL: 0% unsafe SQL generated trên test set
- [ ] Text-to-SQL: accuracy > 80% trên 30-question test set
- [ ] All queries logged với query_hash (để analyze failures)
- [ ] Auto-report PDF generated và readable (không corrupt)
- [ ] Streamlit UI: first response < 10s (bao gồm RAG retrieval)

---

## PHASE 4 — Production & Showcase (Tuần 13–16)

### Tuần 13 — Country Clustering: Interpretability

```python
# Senior insight: Clustering không chỉ cần đúng, còn cần EXPLAINABLE.
# "Tại sao Vietnam nằm cùng cluster với Indonesia?" phải trả lời được.

class CountryClusterer:
    
    def explain_cluster(self, country: str, cluster_id: int) -> ClusterExplanation:
        """
        Trả lời: "Nước này thuộc nhóm vì những đặc điểm gì?"
        """
        cluster_members = self.get_cluster_members(cluster_id)
        
        # Feature importance: SHAP values cho cluster assignment
        shap_values = self.compute_shap(country)
        top_features = sorted(shap_values.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
        
        return ClusterExplanation(
            country=country,
            cluster_id=cluster_id,
            cluster_label=self.cluster_labels[cluster_id],  # e.g. "Lower-middle income, high growth"
            similar_countries=cluster_members[:5],
            defining_features=[
                f"{feat}: {val:.2f}" for feat, val in top_features
            ],
            peer_recommendation=self._recommend_peers(country, cluster_id)
        )
```

### Tuần 14–15 — Production Readiness Checklist

```
SECURITY:
  [ ] API authentication (JWT hoặc API key)
  [ ] Rate limiting (100 req/min per client)
  [ ] SQL injection prevention (parameterized queries only)
  [ ] Secrets không trong code (GCP Secret Manager)
  [ ] CORS configured (chỉ allow known origins)
  [ ] LLM prompt injection mitigation

RELIABILITY:
  [ ] Health check endpoints: /health, /ready, /metrics
  [ ] Graceful shutdown (drain requests trước khi restart)
  [ ] Circuit breaker cho external APIs (WB, OpenAI)
  [ ] Retry với exponential backoff ở mọi external call
  [ ] Fallback responses khi AI unavailable

PERFORMANCE:
  [ ] Redis cache cho model predictions (TTL=24h)
  [ ] Connection pooling cho DB
  [ ] Async endpoints cho long-running tasks
  [ ] Response compression (gzip)

OBSERVABILITY:
  [ ] Structured JSON logging mọi request
  [ ] Prometheus metrics exposed tại /metrics
  [ ] Distributed tracing với OpenTelemetry
  [ ] Alerting rules defined trong Grafana
```

---

## 🧪 Testing Strategy — Senior Grade

### Testing Pyramid

```
                    /\
                   /  \
                  / E2E \         5% — Chậm, đắt, test critical paths
                 /  Tests \
                /──────────\
               /Integration \     25% — Test service boundaries
              /    Tests     \
             /────────────────\
            /   Unit Tests     \  70% — Nhanh, nhiều, test logic
           /────────────────────\
```

### Unit Tests — DE Layer

```python
# tests/unit/test_wb_client.py

class TestWorldBankClient:
    
    def test_fetch_is_idempotent(self, mock_api, mock_manifest):
        """
        Gọi fetch 2 lần với cùng params → chỉ 1 API call.
        Đây là tính chất quan trọng nhất của Bronze layer.
        """
        client = WorldBankClient()
        fetch_id = "test_fetch_001"
        
        # First call
        result1 = client.fetch_indicator("NY.GDP.MKTP.CD", ["VNM"], (2020, 2023), fetch_id)
        assert result1.status == "SUCCESS"
        assert mock_api.call_count == 1
        
        # Second call — phải skip, không gọi API lại
        result2 = client.fetch_indicator("NY.GDP.MKTP.CD", ["VNM"], (2020, 2023), fetch_id)
        assert result2.status == "SKIPPED"
        assert mock_api.call_count == 1   # không tăng!
    
    def test_handles_rate_limit_with_backoff(self, mock_api_rate_limited):
        """
        429 response → retry với backoff → eventually succeed.
        """
        mock_api_rate_limited.side_effect = [
            APIRateLimitError(),
            APIRateLimitError(),
            MockResponse(data=[...])   # thành công ở lần thứ 3
        ]
        
        with patch('time.sleep') as mock_sleep:
            result = client.fetch_indicator(...)
            
        assert result.status == "SUCCESS"
        assert mock_sleep.call_count == 2
        # Verify exponential backoff: 1s, 2s
        assert mock_sleep.call_args_list[0][0][0] == 1
        assert mock_sleep.call_args_list[1][0][0] == 2
    
    def test_schema_change_halts_and_alerts(self, mock_api_new_schema):
        """
        Khi WB thay đổi schema → không fail silently, phải alert.
        """
        with pytest.raises(APISchemaChangedError):
            client.fetch_indicator(...)
        
        assert alert_mock.called
        assert manifest_mock.mark_failed.called
```

### Unit Tests — AI Layer

```python
# tests/unit/test_evaluation.py

class TestWalkForwardValidator:
    
    def test_no_lookahead_bias(self):
        """
        Critical: validation data phải LUÔN sau training data.
        Look-ahead bias = sai lầm nghiêm trọng nhất trong time-series ML.
        """
        data = pd.DataFrame({'year': range(1970, 2024), 'value': ...})
        validator = WalkForwardValidator()
        folds = validator.create_folds(data)
        
        for fold in folds:
            train_years = data[fold.train_mask]['year']
            val_years   = data[fold.val_mask]['year']
            
            assert train_years.max() < val_years.min(), \
                f"Look-ahead bias in fold {fold.fold_id}! " \
                f"Train max={train_years.max()}, Val min={val_years.min()}"

# tests/unit/test_sql_safety.py

class TestSQLValidator:
    
    @pytest.mark.parametrize("unsafe_sql", [
        "DROP TABLE gold.fact_indicators",
        "DELETE FROM gold.fact_indicators WHERE 1=1",
        "INSERT INTO gold.fact_indicators VALUES (...)",
        "SELECT * FROM gold.fact_indicators; DROP TABLE users;",  -- SQL injection
        "SELECT pg_sleep(100)",   -- DoS
    ])
    def test_rejects_unsafe_sql(self, unsafe_sql):
        validator = SQLValidator()
        result = validator.validate(unsafe_sql)
        assert not result.is_safe, f"Should reject: {unsafe_sql}"
```

### Integration Tests

```python
# tests/integration/test_pipeline_e2e.py

class TestPipelineIntegration:
    """
    Test Bronze → Silver → Gold với real (small) dataset.
    Chạy trên CI với mocked external APIs.
    """
    
    @pytest.fixture(autouse=True)
    def setup_test_db(self, tmp_path):
        """Isolated test DB, cleaned after each test."""
        self.db = create_test_database(tmp_path)
        yield
        self.db.cleanup()
    
    def test_bronze_to_gold_pipeline(self, sample_wb_response):
        """End-to-end: raw API response → Gold table."""
        
        # Step 1: Ingest to Bronze
        client = WorldBankClient(db=self.db)
        fetch_result = client.fetch_indicator(
            "NY.GDP.MKTP.CD", ["VNM", "THA"],
            (2018, 2022), "test_fetch_001"
        )
        assert fetch_result.status == "SUCCESS"
        assert self.db.count("bronze.raw_indicators") == 10  # 2 countries × 5 years
        
        # Step 2: dbt transform
        dbt_runner.run(models=["silver_indicators", "gold_fact_indicators"])
        
        # Step 3: Verify Gold quality
        gold_df = self.db.query("SELECT * FROM gold.fact_indicators WHERE country_code = 'VNM'")
        assert len(gold_df) == 5
        assert gold_df['yoy_growth_pct'].notna().all()  # computed features không null
        assert (gold_df['year'] >= 2018).all()
    
    def test_rerun_is_idempotent(self, sample_wb_response):
        """Pipeline chạy 2 lần → kết quả giống nhau, không duplicate."""
        
        for _ in range(2):
            client.fetch_indicator("NY.GDP.MKTP.CD", ["VNM"], (2020, 2022), "test_001")
            dbt_runner.run(models=["silver_indicators"])
        
        count = self.db.count("silver.indicators WHERE country_code = 'VNM'")
        assert count == 3, f"Expected 3 rows, got {count} (possible duplicate!)"
```

### Performance & Load Tests

```python
# tests/load/locustfile.py

class GDIPUser(HttpUser):
    wait_time = between(0.5, 2)
    
    @task(3)
    def get_forecast(self):
        country = random.choice(["VNM", "THA", "IDN", "PHL", "MYS"])
        indicator = "NY.GDP.MKTP.KD.ZG"
        
        with self.client.get(
            f"/v1/forecast/{country}/{indicator}",
            name="/v1/forecast/[country]/[indicator]"   # group metrics
        ) as response:
            if response.elapsed.total_seconds() > 0.5:
                response.failure(f"Too slow: {response.elapsed.total_seconds():.2f}s")
    
    @task(1)
    def chat_query(self):
        with self.client.post("/v1/chat",
            json={"query": "What is GDP of Vietnam?", "language": "en"},
            timeout=15
        ) as response:
            data = response.json()
            if "answer" not in data:
                response.failure("Missing 'answer' in response")

# Targets:
# 100 concurrent users
# 0% error rate
# p50 < 200ms (forecast, cache hit)
# p95 < 500ms (forecast, cache miss)
# p95 < 8s   (chat, RAG pipeline)
```

---

## CI/CD Pipeline

```yaml
# .github/workflows/test.yml

name: GDIP Test Suite

on:
  pull_request:
    branches: [develop, main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff mypy
      - run: ruff check .           # fast linting
      - run: mypy ingestion/ ai/ api/ --ignore-missing-imports

  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/unit/ -v --cov=. --cov-report=xml
      - run: codecov   # fail nếu coverage < 80%

  de-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env: { POSTGRES_PASSWORD: test }
    steps:
      - run: pytest tests/integration/de/ -v
      - run: dbt test --profiles-dir .ci/
      - run: great_expectations checkpoint run silver_checkpoint

  ai-tests:
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/unit/ai/ tests/integration/ai/ -v
      - run: python ai/forecasting/evaluate.py --assert-mape-lt 0.15
      - run: python ai/rag/ragas_eval.py --assert-faithfulness-gt 0.80

  security:
    runs-on: ubuntu-latest
    steps:
      - run: pip install bandit safety
      - run: bandit -r . -ll              # security linting
      - run: safety check                  # dependency vulnerabilities
      - run: grep -r "password\|secret\|api_key" --include="*.py" . | grep -v ".env" | grep -v "test_" && exit 1 || exit 0

  # Chỉ chạy khi merge vào main
  load-test:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - run: locust -f tests/load/locustfile.py --headless -u 100 -r 10 -t 120s
             --exit-code-on-error 1
             --html tests/load/report.html
```


---

## PHASE 3 — AI Agent Layer

> Mục tiêu: Build Multi-Agent System bằng LangGraph. Tự code từng agent — không dùng FinRobot framework. Đủ nhỏ để hiểu hoàn toàn, đủ thực để ghi CV.

### Definition of Ready
- [ ] Tầng Gold data đã có ≥ 10 indicators, ≥ 100 quốc gia
- [ ] RAG pipeline từ Tháng 2 đang chạy (pgvector + hybrid search)
- [ ] ML Classifier từ Tháng 1 đã có API endpoint (`/v1/classify/{country}`)
- [ ] OpenAI API key còn credit (hoặc dùng local LLM Ollama làm fallback)
- [ ] `pip install langgraph` đã cài xong

### Tuần 1-2: Economic Copilot + SQL Agent

**Definition of Done:**
- [ ] `AgentState` TypedDict định nghĩa đầy đủ, có docstring cho mỗi field
- [ ] `route_intent()` phân loại đúng ≥ 90% query trong test set (10 câu hỏi manual)
- [ ] `sql_agent_node()`: chỉ generate SELECT, validated schema, không hallucinate tên cột
- [ ] `rag_agent_node()`: reuse pipeline từ Tháng 2, trả về top-5 chunks
- [ ] LangGraph graph build + compile thành công, không có dangling edge
- [ ] End-to-end test: query "What is Vietnam GDP in 2020?" → trả về đúng số liệu
- [ ] `tests/unit/test_sql_agent.py`: ≥ 5 test cases (valid query, injection attempt, unknown table)

### Tuần 3: Risk Scoring Agent

**Definition of Done:**
- [ ] `RiskComponents` dataclass: 4 components, mỗi cái có công thức tính rõ ràng trong docstring
- [ ] `compute_risk_score()`: test trên 3 known cases
  - Thailand 1997 → score ≥ 70 (crisis)
  - Vietnam 2022 → score 20-40 (stable)
  - Argentina 2001 → score ≥ 80 (crisis)
- [ ] `explain_score()`: output dạng bullet points, interviewer đọc hiểu được ngay
- [ ] ML classifier override hoạt động: nếu XGBoost P(crisis) > 0.7 → boost score +15

### Tuần 4: Country Research Agent + Integration

**Definition of Done:**
- [ ] `country_research_agent()`: generate structured report có 4 sections (Growth, Risk, Forecast, Key Risks)
- [ ] Multi-agent orchestration: Supervisor gọi đúng combination agents tùy intent
- [ ] FastAPI endpoint `POST /v1/agent/query` nhận free-text, trả về JSON response
- [ ] Latency p95 < 5 giây (acceptable với LLM in the loop)
- [ ] `tests/integration/test_agent_e2e.py`: 3 test cases cover 3 intent types

### Tests Quan Trọng nhất

```python
# tests/unit/test_sql_agent.py

class TestSQLAgent:

    def test_rejects_delete_statement(self):
        """SQL Agent không được phép generate DELETE."""
        result = sql_agent_node(AgentState(
            query="Delete all records from feature_store",
            ...
        ))
        assert result["sql_result"]["error"] is not None
        assert "SELECT only" in result["sql_result"]["error"]

    def test_rejects_unknown_table(self):
        """SQL Agent không được query bảng không có trong ALLOWED_TABLES."""
        result = sql_agent_node(AgentState(
            query="Select * from users",
            ...
        ))
        assert "not allowed" in result["sql_result"]["error"].lower()

    def test_returns_correct_vnm_2020_gdp(self):
        """End-to-end: query Vietnam GDP 2020 phải trả về số đúng từ DB."""
        result = sql_agent_node(AgentState(
            query="What is Vietnam GDP growth in 2020?",
            ...
        ))
        # VNM 2020 GDP growth = 2.91% (WB data)
        assert abs(result["sql_result"]["value"] - 2.91) < 0.1

class TestRiskScorer:

    def test_thailand_1997_high_risk(self):
        """Asian Financial Crisis 1997 phải có risk score cao."""
        score = compute_risk_score("THA", 1997)
        total = (score.inflation_score + score.debt_score +
                 score.unemployment_score + score.gdp_decline_score)
        assert total >= 70, f"Thailand 1997 score {total} quá thấp"

    def test_explain_score_readable(self):
        """explain_score() phải trả về string có số liệu cụ thể."""
        components = compute_risk_score("ARG", 2001)
        explanation = explain_score(components)
        assert "inflation" in explanation.lower()
        assert any(char.isdigit() for char in explanation)
```

---

## PHASE 4 — MLOps Layer

> Mục tiêu: Mọi model đều có "giấy khai sinh" (Model Card), được so sánh thống kê trước khi lên Production, và được monitor 3 loại drift liên tục.

### Definition of Ready
- [ ] MLflow tracking server đang chạy và accessible
- [ ] Ít nhất 2 model đã được train và logged vào MLflow (Prophet + XGBoost)
- [ ] Holdout set [2019-2023] vẫn locked — không được touch khi train
- [ ] `thư mục models/cards/` đã tạo sẵn

### Việc 1: Model Card cho mọi model Production

**Definition of Done:**
- [ ] Mỗi model lên Production có 1 file YAML trong `models/cards/`
- [ ] YAML có đủ 7 sections: model_name, algorithm, training, performance, limitations, fairness, deployment
- [ ] `performance.diebold_mariano_vs_arima` field phải có giá trị thật (không được để placeholder)
- [ ] `fairness` section evaluate separately: Low-income vs High-income countries
- [ ] Script `ai/mlops/validate_model_card.py` auto-validate YAML schema trước khi merge PR

### Việc 2: Champion-Challenger Framework

**Definition of Done:**
- [ ] `ChampionChallengerFramework.evaluate_and_promote()` implement đầy đủ
- [ ] Promotion criteria cứng: improvement ≥ 5% MAPE + DM test p < 0.05 + latency < 1000ms
- [ ] Kết quả mỗi lần evaluate được log vào MLflow với tag `evaluation_type=champion_challenger`
- [ ] Airflow DAG `ml_retrain.py` gọi framework này sau mỗi lần retrain
- [ ] `tests/unit/test_champion_challenger.py`:
  - Test case: Challenger tệt hơn → KHÔNG promote
  - Test case: Challenger tốt hơn nhưng DM test không significant → KHÔNG promote
  - Test case: Challenger tốt hơn, DM significant, latency ok → PROMOTE

### Việc 3: 3 loại Drift Monitoring

```python
# tests/ai/test_monitoring.py

class TestDriftMonitoring:

    def test_ks_test_detects_inflation_spike(self):
        """
        Simulate post-COVID inflation spike.
        Baseline: inflation mean=3%, std=2%
        Current: inflation mean=12%, std=5%
        KS-test phải detect significant drift.
        """
        baseline = np.random.normal(3, 2, 1000)
        current = np.random.normal(12, 5, 1000)  # Simulated spike

        monitor = ProductionMonitor()
        report = monitor.check_data_drift(
            pd.DataFrame({'inflation': baseline}),
            pd.DataFrame({'inflation': current})
        )
        assert report.is_drifted == True
        assert report.p_value < 0.05

    def test_psi_detects_prediction_shift(self):
        """
        PSI > 0.2 phải trigger alert.
        Baseline: 95% predict 'stable', 5% predict 'crisis'
        Current:  60% predict 'stable', 40% predict 'crisis'
        """
        baseline_preds = [0] * 950 + [1] * 50   # 5% crisis
        current_preds  = [0] * 600 + [1] * 400  # 40% crisis

        monitor = ProductionMonitor()
        report = monitor.check_prediction_drift(current_preds)
        assert report.psi > 0.2
        assert report.severity == "HIGH"
```

### Definition of Done — Phase 4
- [ ] ≥ 3 Model Cards YAML đầy đủ (Prophet, XGBoost, LightGBM)
- [ ] Champion-Challenger chạy được end-to-end với 2 models thật
- [ ] `ProductionMonitor` implement đủ 3 loại drift với alert thresholds documented
- [ ] Grafana dashboard có ≥ 5 panels: prediction distribution, feature drift score, model latency, champion MAPE trend, drift alerts
- [ ] `tests/ai/test_monitoring.py`: ≥ 6 test cases, cover cả 3 loại drift
- [ ] CI/CD: `model_card_validate` step trong GitHub Actions chặn merge nếu Model Card thiếu field

### Benchmark Targets — Phase 3 + 4

| Component | Metric | Target |
|-----------|--------|--------|
| SQL Agent | Query accuracy (manual eval) | ≥ 90% |
| SQL Agent | Rejection rate của invalid SQL | 100% |
| Risk Scorer | Thailand 1997 score | ≥ 70/100 |
| Risk Scorer | Vietnam 2022 score | 20–40/100 |
| Country Research Agent | Report generation latency | < 8s |
| Champion-Challenger | False promotion rate | 0% |
| Drift Monitor (Data) | KS-test recall | ≥ 95% |
| Drift Monitor (Prediction) | PSI false alarm rate | < 5% |
