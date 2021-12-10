from datetime import datetime, timedelta
from time import sleep
import coiled
import dask
from dask.distributed import Client
import dask.dataframe as dd
import matplotlib.pyplot as plt

from airflow.decorators import dag, task

# set default arguments to all tasks
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

storage_directory = ""

# define DAG as a function with the @dag decorator
@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['coiled-demo'],
    )
def airflow_on_coiled():
    """
    A workflow that demonstrates launching a Coiled cluster from within an Airflow task.
    The first "transform" task runs on Coiled, the rest run locally.
    """
    
    # define Airflow task that runs a computation on a Coiled cluster
    @task()
    def transform():
        """
        Perform heavy computation on a large-scale dataset (~70GB) on a Coiled cluster.
        Provisioning cluster resources takes ~2 minutes.
        Returns a local pandas Series containing the number of entries (PushEvents) per user.
        """
        
        # Create and connect to Coiled cluster using the default software environment
        cluster = coiled.Cluster(
            n_workers=20, 
            name="airflow-task",
            software="coiled-examples/airflow",
            backend_options={'spot': 'True'},
        )
        client = Client(cluster)
        print("Dashboard:", client.dashboard_link)

        # Read CSV data from S3
        ddf = dd.read_parquet(
            's3://coiled-datasets/github-archive/github-archive-2015.parq/',
            storage_options={"anon": True, 'use_ssl': True},
            blocksize="16 MiB",
            engine='fastparquet',
            compression='lz4',
        )

        # Compute result number of entries (PushEvents) per user
        result = ddf.user.value_counts().compute()
        
        # Shutdown Coiled cluster
        cluster.close()
        return result

    # define subsequent Airflow tasks without a Coiled cluster
    @task()
    def summarize(series):
        """
        Calculate summary statistics over the pandas Series of interest and save to CSV.
        Runs without Coiled.
        """
        # Get summary statistics
        sum_stats = series.describe()
        # Save to CSV
        sum_stats.to_csv(f'{storage_directory}usercounts_summary_statistics.csv')
        return sum_stats

    @task()
    def get_top_users(series):
        """
        Get user and author names of top 100 most active users and save to CSV.
        Runs without Coiled.
        """
        # Get top 100 most active users
        top_100 = series.head(100)

        # Store user + number of events to CSV
        top_100.to_csv(f'{storage_directory}top_100_users.csv')
        return top_100


    # Call task functions in order
    series = transform()
    stats = summarize(series)
    top_100 = get_top_users(series)

# Call taskflow
run = airflow_on_coiled()