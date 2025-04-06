from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Path ke data CSV
DATA_DIR = '../config/data/'

# 1. Cek NULL
def check_nulls():
    df = pd.read_csv(os.path.join(DATA_DIR, 'factStocks.csv'))
    null_counts = df[['keyTime', 'keyCompany', 'openValueStock', 'closeValueStock', 'quantityStock']].isnull().sum()
    if null_counts.any():
        raise ValueError(f"Ditemukan nilai NULL: \n{null_counts}")

# 2. Cek nilai numerik tidak negatif
def check_non_negative():
    df = pd.read_csv(os.path.join(DATA_DIR, 'factStocks.csv'))
    numeric_cols = ['openValueStock', 'closeValueStock', 'highValueStock', 'lowValueStock', 'quantityStock']
    for col in numeric_cols:
        if (df[col] < 0).any():
            raise ValueError(f"Nilai negatif ditemukan di kolom {col}")

# 3. Cek duplikat keyTime + keyCompany
def check_duplicates():
    df = pd.read_csv(os.path.join(DATA_DIR, 'factStocks.csv'))
    if df.duplicated(subset=['keyTime', 'keyCompany']).any():
        raise ValueError("Terdapat duplikat pada kombinasi keyTime dan keyCompany")

# 4. Cek referensial keyCompany terhadap dimCompany.csv
def check_foreign_keys():
    df_facts = pd.read_csv(os.path.join(DATA_DIR, 'factStocks.csv'))
    df_company = pd.read_csv(os.path.join(DATA_DIR, 'dimCompany.csv'))
    kode_terdaftar = set(df_company['keyCompany'].unique())
    kode_dipakai = set(df_facts['keyCompany'].unique())
    kode_invalid = kode_dipakai - kode_terdaftar
    if kode_invalid:
        raise ValueError(f"keyCompany berikut tidak ditemukan di dimCompany.csv: {kode_invalid}")

# DAG Definition
default_args = {
    'start_date': datetime(2025, 3, 22),
    'retries': 1
}

dag = DAG(
    'data_quality_brazil_stock',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='DAG untuk mengecek kualitas data saham Brazil dari factStocks.csv'
)

check_nulls_task = PythonOperator(
    task_id='check_nulls',
    python_callable=check_nulls,
    dag=dag
)

check_non_negative_task = PythonOperator(
    task_id='check_non_negative',
    python_callable=check_non_negative,
    dag=dag
)

check_duplicates_task = PythonOperator(
    task_id='check_duplicates',
    python_callable=check_duplicates,
    dag=dag
)

check_foreign_keys_task = PythonOperator(
    task_id='check_foreign_keys',
    python_callable=check_foreign_keys,
    dag=dag
)

# Task Dependencies
check_nulls_task >> check_non_negative_task >> check_duplicates_task >> check_foreign_keys_task
