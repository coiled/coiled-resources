from datetime import datetime, timedelta
import coiled
from dask.distributed import Client
import dask.dataframe as dd

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'coiled-demo',
    default_args=default_args,
    description='Coiled Airflow demo',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021,11,9),
    catchup=False,
    tags=['demo'],
) as dag:

    t1 = PythonOperator(
        task_id='read_data',
        python_callable=...
    )