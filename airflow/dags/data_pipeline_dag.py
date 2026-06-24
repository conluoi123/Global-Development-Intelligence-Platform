from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.append(DAG_DIR)

from transform.clean_silver import clean_and_impute_data
from transform.feature_engineering import run_feature_engineering

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
    'gdip_data_pipeline',
    default_args=default_args,
    description='ETL Pipeline: Clean Silver Layer & Feature Engineering Gold Layer',
    schedule_interval=None,  # Chạy thủ công hoặc trigger từ API
    catchup=False,
    tags=['gdip', 'etl'],
) as dag:

    clean_silver_task = PythonOperator(
        task_id='clean_silver_layer',
        python_callable=clean_and_impute_data,
    )

    feature_engineering_task = PythonOperator(
        task_id='feature_engineering_gold_layer',
        python_callable=run_feature_engineering,
    )

    # Tự động trigger DAG suy luận AI khi ETL hoàn tất thành công
    trigger_ml_inference = TriggerDagRunOperator(
        task_id='trigger_ml_inference_dag',
        trigger_dag_id='gdip_ml_inference',
        wait_for_completion=False,
    )

    clean_silver_task >> feature_engineering_task >> trigger_ml_inference
