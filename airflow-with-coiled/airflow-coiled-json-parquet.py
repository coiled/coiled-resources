from datetime import datetime, timedelta
from time import sleep
import coiled
import dask
from dask.distributed import Client
import dask.dataframe as dd
import dask.bag as db
import ujson
import boto3

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

# define path to S3 bucket where we'll store the Parquet file
path = "<s3://bucket/filename.parquet>"


# define DAG as a function with the @dag decorator
@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['coiled-demo'],
    )
def json_to_parquet():
    """
    A workflow that converts a large JSON file to Parquet.
    """
    
    @task()
    def create_list(start_date, end_date, format="%d-%m-%Y"):
        '''
        This task generates a list of filenames to fetch from the Github Archive API.
        start_date: a string containing the start date
        end_date: a string containing the end date
        format: datetime format, defaults to "%d-%m-%Y"
        '''
        start = datetime.strptime(start_date, format)
        end = datetime.strptime(end_date, format)
        date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
        prefix = "https://data.gharchive.org/"
        filenames = []
        for date in date_generated:
            for hour in range(1,24):
                filenames.append(prefix + date.strftime("%Y-%m-%d") + '-' + str(hour) + '.json.gz')
        return filenames
    
    # get data from Github api
    @task()
    def transform_github_data(filenames):
        '''
        Task to fetch JSON data from Github Archive project, filter out PushEvents, flatten, and store as Parquet.
        filenames: list of filenames created with create_list() task
        '''

        # Create and connect to Coiled cluster
        cluster = coiled.Cluster(
            n_workers=20, 
            name="airflow-json",
            software="coiled-examples/airflow",
            backend_options={'spot': 'True'},
        )
        client = Client(cluster)
        print("Dashboard:", client.dashboard_link)

        records = db.read_text(filenames).map(ujson.loads)
        push_events = records.filter(lambda record: record["type"] == "PushEvent")

        def process(record):
                try:
                    for commit in record["payload"]["commits"]:
                        yield {
                            "user": record["actor"]["login"],
                            "repo": record["repo"]["name"],
                            "created_at": record["created_at"],
                            "message": commit["message"],
                            "author": commit["author"]["name"],
                        }
                except KeyError:
                    pass
            
        processed = push_events.map(process)
        df = processed.flatten().to_dataframe()
        df.to_parquet(
            path,
            engine='fastparquet',
            compression='lz4',
            storage_options={
                "key":'<aws_access_key>', 
                "secret":'<aws_secret_key>'
            }
        )
        cluster.close()
        return df


    # Call task functions in order
    start_date = "01-01-2015"
    end_date = "31-12-2015"
    files_to_fetch = create_list(start_date, end_date)
    dataframe = transform_github_data(files_to_fetch)

# Call taskflow
demo = json_to_parquet()