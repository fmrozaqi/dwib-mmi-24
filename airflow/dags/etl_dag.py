from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from components.pipeline import ETLPipeline
from components.converter import convert_duckdb_to_sqlite

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

def run_full_pipeline():
    pipeline = ETLPipeline()
    pipeline.run_full_pipeline()

def update_coin_data():
    pipeline = ETLPipeline()
    pipeline.insert_fact_data(
        csv_path="config/data/generated/coin_values.csv",
        staging_fact_table="stagingFactCoins",
        dim_table="dimCoin",
        key_column="keyCoin",
        dimension_lookup_column="abbrevCoin"
    )

def update_stock_data():
    pipeline = ETLPipeline()
    pipeline.insert_fact_data(
        csv_path="config/data/generated/stock_values.csv",
        staging_fact_table="stagingFactStocks",
        dim_table="dimCompany",
        key_column="keyCompany",
        dimension_lookup_column="stockCodeCompany"
    )

with DAG(
    'brazil_stock_market_etl',
    default_args=default_args,
    description='ETL pipeline for Brazil Stock Market data',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'stocks'],
) as dag:

    # First run task - initializes the warehouse and loads all data
    full_pipeline = PythonOperator(
        task_id='run_full_pipeline',
        python_callable=run_full_pipeline,
    )

    # Daily update tasks
    update_coins = PythonOperator(
        task_id='update_coin_data',
        python_callable=update_coin_data,
    )

    update_stocks = PythonOperator(
        task_id='update_stock_data',
        python_callable=update_stock_data,
    )
    
    # DuckDB to SQLite conversion task
    convert_to_sqlite = PythonOperator(
        task_id='convert_to_sqlite',
        python_callable=convert_duckdb_to_sqlite,
        op_kwargs={
            'input_path': 'config/brazil_stock_market.db',
            'output_path': 'config/metabase/brazil_stock_market_sqlite.db'
        },
    )

    # Set up dependencies
    full_pipeline >> update_coins >> update_stocks >> convert_to_sqlite