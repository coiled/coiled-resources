from datetime import datetime, timedelta
from time import sleep
import coiled
from dask.distributed import Client
import dask.dataframe as dd

from airflow.decorators import dag, task

# set default arguments to all tasks
default_args = {
    'owner': 'rrpelgrim',
    'depends_on_past': False,
    'email': ['richard@coiled.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# define DAG as a function with the @dag decorator
@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['coiled-demo'],
    )
def coiled_airflow_task():
    """
    A workflow that launches a Coiled cluster from within an Airflow task.
    """
    
    # define Airflow task that runs a computation on a Coiled cluster
    @task()
    def transform():
        """
        Running a groupby on a large CSV using Coiled.
        """
        # Create and connect to Coiled cluster
        cluster = coiled.Cluster(
            n_workers=10, 
            name="airflow-task",
        )
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

    # define an Airflow task without a Coiled cluster
    @task()
    def clean(series):
        """
        Airflow task without Coiled.
        """
        # Filter for 2 or more passengers
        series = series[series.index > 0]
        # Round tip amount to 2 decimal places
        series = round(series, 2)
        return series

    # call task functions in order
    series = transform()
    cleaned = clean(series)

# call taskflow
demo_taskflow = coiled_airflow_task()