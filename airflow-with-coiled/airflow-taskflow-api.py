from datetime import datetime, timedelta
from time import sleep
import coiled
from dask.distributed import Client
import dask.dataframe as dd

from airflow.decorators import dag, task


default_args = {
    'owner': 'rrpelgrim',
    'depends_on_past': False,
    'email': ['richard@coiled.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['coiled-demo'],
    )
def coiled_airflow_taskflow():
    """
    Just trying this out...
    """
    @task()
    def print_something():
        print('something')
        sleep(3)

    @task()
    def extract():
        """
        A sample Dask task on Coiled...
        """
        # Create and connect to Coiled cluster
        cluster = coiled.Cluster(n_workers=10, name="airflow-task")
        client = Client(cluster)
        print("Dashboard:", client.dashboard_link)

        # Read CSV data from S3
        df = dd.read_csv(
            "s3://nyc-tlc/trip data/yellow_tripdata_2019-*.csv",
            dtype={
                "payment_type": "UInt8",
                "VendorID": "UInt8",
                "passenger_count": "UInt8",
                "RatecodeID": "UInt8",
            },
            storage_options={"anon": True},
            blocksize="16 MiB",
        ).persist()

        # Compute result
        result = df.groupby("passenger_count").tip_amount.mean().compute()
        return result

    @task()
    def print_result(result):
        print(result)
        sleep(3)

    # call task functions in order
    print_something = print_something()
    result = extract()
    print_result = print_result(result)

# call taskflow
demo_taskflow = coiled_airflow_taskflow()
