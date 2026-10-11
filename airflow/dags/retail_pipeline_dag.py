from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 

PROJECT_ROOT = "/opt/airflow/retail_pipeline"

default_args = {
    "owner": "krish",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="retail_analytics_pipeline",
    default_args=default_args,
    description="Bronze -> Silver -> Anomaly Detection -> Gold",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    tags=["retail", "medallion", "pyspark"],
    catchup=False
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PROJECT_ROOT} && python pipeline/bronze/bronze_ingestion.py"
    )

    silver_task = BashOperator(
        task_id="silver_processing",
        bash_command=f"python {PROJECT_ROOT}/pipeline/silver/silver_processing.py"
    )

    anomaly_task = BashOperator(
            task_id="anomaly_detection",
            bash_command=f"python {PROJECT_ROOT}/pipeline/anomaly/anomaly_detection.py"
        )

    gold_task = BashOperator(
        task_id="gold_processing",
        bash_command=f"python {PROJECT_ROOT}/pipeline/gold/gold_processing.py"
    )

    bronze_task >> silver_task >> anomaly_task >> gold_task