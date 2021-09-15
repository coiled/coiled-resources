import datetime
import coiled
from dask.distributed import Client, worker_client 
import dask.bag as db
import ujson

from prefect import task, Flow, resource_manager, Task, case, Parameter


# #SET-UP
# #spin up cluster function
# @task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
# def start_cluster(filenames, cc_name="prefect", cc_software="coiled-examples/prefect", cc_n_workers=10):
#     '''
#     This task spins up a Coiled cluster and connects it to the Dask client.

#     filenames: list of filenames created by create_list()
#     cc_name: name of the Coiled cluster, defaults to "prefect"
#     cc_software: Coiled software environment to install on all workers, defaults to "coiled-examples/prefect"
#     cc_n_workers: number of Dask workers in the Coiled cluster, defaults to 10
#     '''
#     if len(filenames) < 50:
#         return Client(processes=False)

#     else:
#         cluster = coiled.Cluster(
#             name=cc_name,
#             software=cc_software,
#             n_workers=cc_n_workers,
#             )
#         return Client(cluster)

# create list of filenames to fetch
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def create_list(start_date, end_date, format="%d-%m-%Y"):
    '''
    This task generates a list of filenames to fetch from the Github Archive API.

    start_date: a string containing the start date
    end_date: a string containing the end date
    format: datetime format, defaults to "%d-%m-%Y"
    '''
    start = datetime.datetime.strptime(start_date, format)
    end = datetime.datetime.strptime(end_date, format)
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
    prefix = "https://data.gharchive.org/"
    filenames = []
    for date in date_generated:
        for hour in range(1,24):
            filenames.append(prefix + date.strftime("%Y-%m-%d") + '-' + str(hour) + '.json.gz')
    return filenames

# EXTRACT
# get data from Github api
@task(checkpoint=False)
def get_github_data(filenames):
    '''
    Task to fetch JSON data from Github Archive project and filter out PushEvents.

    filenames: list of filenames created with create_list() task
    '''

    records = db.read_text(filenames).map(ujson.loads)
    push_events = records.filter(lambda record: record["type"] == "PushEvent")
    return push_events


# TRANSFORM 
# transform json into dataframe
@task(checkpoint=False)
def to_dataframe(push_events):
    '''
    This task processes the nested json data into a flat, tabular format. 
    Each row represents a single commit.

    push_events: PushEvent data fetched with get_github_data()
    '''
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
    return df


# LOAD
# write to parquet
@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def to_parquet(df, path="s3://coiled-datasets/prefect/conditional-test.parq"):
    '''
    This task writes the flattened dataframe of PushEvents to the specified path as a parquet file.

    path: directory to write parquet file to, could be local or cloud storage
    '''
    df.to_parquet(
        path,
        engine='fastparquet',
        compression='lz4'
    )

@task
def determine_cluster_type(filenames):
    if len(filenames) > 50:
        return "coiled"
    return "local"

@resource_manager
class DaskCluster:
    def __init__(self, cluster_type="local", n_workers=None, software=None, account=None, name=None):
        self.cluster_type = cluster_type
        self.n_workers = n_workers
        self.software = software 
        self.account = account
        self.name = name

    def setup(self):
        if self.cluster_type == "local":
            return Client(processes=False)
        elif self.cluster_type == "coiled":
            cluster = coiled.Cluster(
                name = self.name,
                software = self.software,
                n_workers = self.n_workers,
                account = self.account,
            )
            return Client(cluster)
    
    def cleanup(self, client):
        client.close()
        if self.cluster_type == "coiled":
            client.cluster.close()


# Build Prefect Flow
with Flow(name="Github ETL Test") as flow:
    # define parameters
    n_workers = Parameter("n_workers", default=4)
    software = Parameter("software", default='coiled-examples/prefect')
    account = Parameter("account", default=None)
    name = Parameter("name", default='cluster-name')
    start_date = Parameter("start_date", default="01-01-2015")
    end_date = Parameter("end_date", default="31-12-2015")

    # build flow
    filenames = create_list(start_date=start_date, end_date=end_date)
    cluster_type = determine_cluster_type(filenames)
    
    with DaskCluster(
            cluster_type=cluster_type,
            n_workers=n_workers,
            software=software,
            account=account,
            name=name
            ) as client:
        push_events = get_github_data(filenames)
        df = to_dataframe(push_events)
        to_parquet(df)


# Run flow with parameters
flow.run(
    parameters=dict(
        end_date="02-01-2015",
        n_workers=15,
        name="prefect-on-coiled")
)