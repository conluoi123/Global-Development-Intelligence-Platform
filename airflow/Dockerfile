FROM apache/airflow:2.9.1 

# Cài đặt thư viện hệ thống cần thiết cho LightGBM/XGBoost dưới quyền root
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Trả lại quyền user airflow để cài đặt thư viện python
USER airflow
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt