"""
DAG для анализа рынка криптовалют
Вариант задания №30

Автор: Студент
Дата: 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os
import shutil
import pandas as pd
import kagglehub

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'crypto_market_analysis',
    default_args=default_args,
    description='Анализ криптовалютного рынка - вариант 30',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'crypto', 'kaggle', 'variant_30']
)

def extract_crypto_data(**context):
    """
    Extract: Скачивание данных о BTC с Kaggle
    """
    DATA_DIR = '/opt/airflow/dags/data'
    os.makedirs(DATA_DIR, exist_ok=True)
    
    kaggle_json_path = '/home/airflow/.kaggle/kaggle.json'
    if not os.path.exists(kaggle_json_path):
        raise FileNotFoundError(f"kaggle.json не найден в {kaggle_json_path}")
    
    dataset_name = "mczielinski/bitcoin-historical-data"
    print(f"Скачиваем датасет: {dataset_name}")
    path = kagglehub.dataset_download(dataset_name)
    print(f"Данные скачаны в: {path}")
    
    # Ищем CSV-файл
    csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("В датасете не найдено CSV-файлов")
    
    source_file = os.path.join(path, csv_files[0])
    dest_file = os.path.join(DATA_DIR, "btcusd_1-min_data.csv")
    shutil.copy2(source_file, dest_file)
    print(f"Файл скопирован: {dest_file}")
    
    context['task_instance'].xcom_push(key='data_file_path', value=dest_file)

def load_raw_btc_to_postgres(**context):
    """
    Load: Загрузка первых 100 000 строк сырых данных BTC в PostgreSQL
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    csv_file_path = context['task_instance'].xcom_pull(
        key='data_file_path',
        task_ids='extract_crypto_data'
    )
    if not csv_file_path:
        raise FileNotFoundError("Путь к CSV-файлу не найден в XCom")

    print(f"Чтение первых 100 000 строк из: {csv_file_path}")
    
    # Загружаем только первые 100 000 строк
    df = pd.read_csv(csv_file_path, nrows=100_000)
    
    df.rename(columns={'Timestamp': 'timestamp', 'Close': 'close_price'}, inplace=True)
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='s')
    
    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    table_name = "raw_btc_prices"
    
    postgres_hook.run(f"DROP TABLE IF EXISTS {table_name};")
    create_sql = f"""
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        timestamp BIGINT,
        date TIMESTAMP,
        close_price NUMERIC
    );
    """
    postgres_hook.run(create_sql)
    
    rows = df[['timestamp', 'date', 'close_price']].values.tolist()
    postgres_hook.insert_rows(
        table=table_name,
        rows=rows,
        target_fields=['timestamp', 'date', 'close_price']
    )
    print(f"Успешно загружено {len(rows)} записей в {table_name}")

def transform_and_enrich_data(**context):
    """
    Transform: Создание витрины с BTC и заглушкой для ETH
    """
    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    
    # Получаем данные BTC
    btc_df = postgres_hook.get_pandas_df("SELECT date, close_price AS btc_price_close FROM raw_btc_prices;")
    
    # Создаём заглушку для ETH (условно: 5% от цены BTC)
    btc_df['eth_price_close'] = btc_df['btc_price_close'] * 0.05
    
    # Условная капитализация BTC: ~19M монет
    btc_df['btc_market_cap'] = btc_df['btc_price_close'] * 19_000_000
    
    # Оставляем только нужные поля
    final_df = btc_df[['date', 'btc_price_close', 'eth_price_close', 'btc_market_cap']].copy()
    
    # Загружаем в staging
    postgres_hook.run("DROP TABLE IF EXISTS stg_crypto_market CASCADE;")
    create_sql = """
    CREATE TABLE stg_crypto_market (
        date TIMESTAMP,
        btc_price_close NUMERIC,
        eth_price_close NUMERIC,
        btc_market_cap NUMERIC
    );
    """
    postgres_hook.run(create_sql)
    
    rows = final_df.values.tolist()
    postgres_hook.insert_rows(
        table='stg_crypto_market',
        rows=rows,
        target_fields=['date', 'btc_price_close', 'eth_price_close', 'btc_market_cap']
    )
    print(f"Загружено {len(rows)} записей в stg_crypto_market")

# Задачи
extract_task = PythonOperator(
    task_id='extract_crypto_data',
    python_callable=extract_crypto_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_raw_btc_to_postgres',
    python_callable=load_raw_btc_to_postgres,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_and_enrich_data',
    python_callable=transform_and_enrich_data,
    dag=dag
)

create_datamart_task = PostgresOperator(
    task_id='create_datamart',
    postgres_conn_id='analytics_postgres',
    sql='datamart_variant_25.sql',
    dag=dag
)

extract_task >> load_task >> transform_task >> create_datamart_task