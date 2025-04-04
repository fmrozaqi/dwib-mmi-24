from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define your task functions
def extract():
    print("🔍 Extracting data...")

def transform():
    print("🔄 Transforming data...")

def load():
    print("📥 Loading data...")

def notify():
    print("📨 Sending notification...")

# Define the DAG
with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2025, 3, 22),
    schedule_interval=None,
    catchup=False,
    tags=["example", "etl"]
) as dag:

    # Tasks
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    notify_task = PythonOperator(
        task_id="notify_task",
        python_callable=notify
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task >> notify_task
