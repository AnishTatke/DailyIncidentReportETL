from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG("export_metrics_dag", start_date=datetime(2023, 1, 1), schedule_interval="@hourly", catchup=False):
    EmptyOperator(task_id="export_metrics")