from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'malhar',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_and_upload():
    subprocess.run([
        'python3',
        '/home/mardy/airflow/dags/get_data_auto.py'
    ], check=True)

def run_dbt():
    subprocess.run([
        '/home/mardy/airflow_env/bin/dbt', 'run',
        '--project-dir',
        '/mnt/c/Users/mardy/OneDrive/Documents/Porftolio-Risk-Analysis/Python/portfolio_pipeline',
        '--profiles-dir',
        '/mnt/c/Users/mardy/.dbt'
    ], check=True)

COPY_SQL = """
    COPY INTO RAW_HISTORICAL_PRICES_WIDE
    FROM 's3://malharportfolioriskanalysis/Data/historical_prices.csv'
    STORAGE_INTEGRATION = S3_PORTFOLIO_INT
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    PURGE = FALSE
"""

with DAG(
    dag_id='portfolio_pipeline',
    default_args=default_args,
    description='Daily ETF pipeline: download → S3 → Snowflake → dbt',
    schedule='0 8 * * *',
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=['portfolio', 'etf', 'snowflake'],
) as dag:

    task_download = PythonOperator(
        task_id='download_and_upload_to_s3',
        python_callable=download_and_upload,
    )

    task_load = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql=COPY_SQL,
        snowflake_conn_id='snowflake_default',
    )

    task_dbt = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt,
    )

    task_download >> task_load >> task_dbt
