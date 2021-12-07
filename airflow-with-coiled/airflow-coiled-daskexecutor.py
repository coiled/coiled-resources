from datetime import datetime, timedelta
from time import sleep
import coiled
import dask
from dask.distributed import Client
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns

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

storage_directory = "s3://coiled-datasets/airflow/"

# define DAG as a function with the @dag decorator
@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['coiled-demo'],
    )
def airflow_on_coiled_dask_executor():
    """
    A workflow that runs entirely on a Coiled cluster. 
    The cluster will scale adaptively based on the workload.
    """
    
    # define Airflow task that runs a computation on a Coiled cluster
    @task()
    def transform():
        """
        Perform heavy computation on a large-scale dataset (~70GB) on a Coiled cluster.
        Provisioning cluster resources takes ~2 minutes.
        Returns a local pandas Series containing the number of entries (PushEvents) per user.
        """
        
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

    
    @task()
    def visualize(series, sum_stats):
        """
        Create visualisation plots and save as PNGs.
        Runs without Coiled.
        """
        # Create boxplot for users within 75, 90, 95, 99, and 99.9% quantiles
        for i in [0.75, 0.90, 0.95, 0.99, 0.999]:
            quantile = i
            fig, ax = plt.subplots(figsize=(10,10)) 
            plt.title(f'# of Push Events per User for Users within {quantile*100}% Quantile', fontsize=15)
            sns.boxplot(
                data=series[series < series.quantile(quantile)],
            )
            plt.axhline(sum_stats['mean'], c='red', label='overall mean')
            plt.savefig(f'boxplot_{quantile*100}quantile.png')

    # Call task functions in order
    series = transform()
    sum_stats = summarize(series)
    top_100 = get_top_users(series)
    visualize(series, sum_stats)

# Call taskflow
demo = airflow_on_coiled_dask_executor()