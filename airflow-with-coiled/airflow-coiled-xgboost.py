from datetime import datetime, timedelta
from dask.distributed import Client
import dask.dataframe as dd
import xgboost as xgb
from dask_ml.preprocessing import Categorizer
from sklearn.metrics import mean_absolute_error
from dask_ml.model_selection import train_test_split

from airflow.decorators import dag, task

# set default arguments to all tasks
default_args = {
    'owner': 'rpelgrim',
    'email': 'rpelgrim@coiled.io',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# define path where we'll store any exported files
path = ""

# define DAG as a function with the @dag decorator
@dag(
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['coiled-demo'],
    )
def train_xgboost():
    """
    A workflow that converts a large JSON file to Parquet.
    """

    @task()
    def create_client(n_workers=4):
        '''
        This task created a local Dask client.
        n_workers: number of local threads to use, defaults to 4.
        '''
        # create local dask cluster
        cluster = LocalCluster(n_workers=n_workers)
        client = Client(cluster)
        return client
    
    @task()
    def load_data():
        '''
        This task loads a subset of the ARCOS dataset that fits into local memory (~250MB).
        client: Dask client object returned by create_client() function.
        '''
        # define the columns we want to import
        columns = [
            "QUANTITY",
            "CALC_BASE_WT_IN_GM",
            "DOSAGE_UNIT",
        ]

        categorical = [
            "REPORTER_BUS_ACT",
            "REPORTER_CITY",
            "REPORTER_STATE",
            "REPORTER_ZIP",
            "BUYER_BUS_ACT",
            "BUYER_CITY",
            "BUYER_STATE",
            "BUYER_ZIP",
            "DRUG_NAME",
        ]

        # download data from S3
        data = dd.read_parquet(
            "s3://coiled-datasets/dea-opioid/arcos_washpost_comp.parquet", 
            compression="lz4",
            storage_options={"anon": True, 'use_ssl': True},
            columns=columns+categorical,
        )

        # select the first 50 partitions
        subset = data.partitions[0:50].compute()
        return subset

    @task()
    def preprocess(subset):
        '''
        This task preprocesses the data, incl. removing NaNs and casting categorical columns to correct dtypes.
        '''
        subset.DOSAGE_UNIT = subset.DOSAGE_UNIT.fillna(value=0)
        # cast categorical columns to the correct type
        ce = Categorizer(columns=categorical)
        subset = ce.fit_transform(subset)
        for col in categorical:
            subset[col] = subset[col].cat.codes
        
        # rearrange columns
        cols = subset.columns.to_list()
        cols_new = [cols[0]] + cols[2:] + [cols[1]]
        subset = subset[cols_new]
        return subset

    @task()
    def train_test_split(subset_clean):
        '''
        This task creates the train and test splits.
        subset_clean: output of preprocess() task.
        '''
        # Create the train-test split
        X, y = subset.iloc[:, :-1], subset["CALC_BASE_WT_IN_GM"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, shuffle=True, random_state=21
        )
        return X_train, X_test, y_train, y_test

    @task()
    def create_DaskDMatrices(X_train, X_test, y_train, y_test):
        '''
        This task creates the Dask DMatrices necessary for training XGBoost model
        '''
        # Create the XGBoost DMatrix for our training and testing splits
        dtrain = xgb.dask.DaskDMatrix(client, X_train, y_train)
        dtest = xgb.dask.DaskDMatrix(client, X_test, y_test)

    @task()
    def train_xgboost(dtrain, dtest)
        '''
        This task trains XGboost with the provided parameters.
        '''
        # Set model parameters (XGBoost defaults)
        params = {
            "max_depth": 6,
            "gamma": 0,
            "eta": 0.3,
            "min_child_weight": 30,
            "objective": "reg:squarederror",
            "grow_policy": "depthwise"
        }

        # train the model
        output = xgb.dask.train(
            client, params, dtrain, num_boost_round=5,
            evals=[(dtrain, 'train')]
        )

        # save model
        ...

        return output

    @task()
    def predict(client, output, dtest):
        '''
        Use trained model to make predictions.
        client: Dask client, output of create_client() task.
        output: XGBoost model, output of train_xgboost() task.
        dtest: test DaskDMatrix, output of create_DaskDMatrices() task.
        '''
        # make predictions
        y_pred = xgb.dask.predict(client, output, dtest)

        # store predictions
        ...

        # evaluate predictions
        mae = mean_absolute_error(y_test, y_pred)
        print(f"Mean Absolute Error: {mae}")



    

    # Call task functions in order
    start_date = "01-01-2015"
    end_date = "31-12-2015"
    files_to_fetch = create_list(start_date, end_date)
    dataframe = transform_github_data(files_to_fetch)
    n_events = count_push_events(dataframe)

# Call taskflow
demo = train_xgboost()