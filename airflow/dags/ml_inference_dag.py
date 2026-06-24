from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.append(DAG_DIR)

from ai.predict_risk import predict_macro_risk
from ai.forecast_macro import forecast_macro_indicators

default_args = {
    'owner': 'gdip_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gdip_ml_inference',
    default_args=default_args,
    description='AI Inference & Time Series Forecasting Pipeline',
    schedule_interval=None,  # Được trigger bởi gdip_data_pipeline hoặc chạy thủ công
    catchup=False,
    tags=['gdip', 'ml', 'forecast'],
) as dag:

    predict_risk_task = PythonOperator(
        task_id='predict_risk_next_year',
        python_callable=predict_macro_risk,
    )

    forecast_macro_task = PythonOperator(
        task_id='forecast_macro_5y',
        python_callable=forecast_macro_indicators,
    )

    # cho chạy song song 
    [predict_risk_task, forecast_macro_task]
