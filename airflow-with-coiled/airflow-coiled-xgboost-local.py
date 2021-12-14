from datetime import datetime, timedelta
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import xgboost as xgb
from dask_ml.preprocessing import Categorizer
from sklearn.metrics import mean_absolute_error
from dask_ml.model_selection import train_test_split
from numpy import save

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

# define path where we'll store any exported files
path = "<path/to/directory>"


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
    A workflow that trains an XGBoost model on ~250MB of data.
    """    
    @task()
    def load_data():
        '''
        This task loads a subset of the ARCOS dataset that fits into local memory (~250MB).
        '''
        # create Dask cluster
        cluster = LocalCluster(n_workers=4)
        client = Client(cluster)

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
        # fill NaNs 
        subset.DOSAGE_UNIT = subset.DOSAGE_UNIT.fillna(value=0)
        
        # cast categorical columns to the correct type
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
    def train_xgboost(subset_clean):
        '''
        This task creates the train and test splits.
        subset_clean: cleaned dataframe, output of preprocess() task.
        '''

        # Create the train-test split
        features = subset_clean.iloc[:, :-1]
        target = subset_clean["CALC_BASE_WT_IN_GM"]
        X_train, X_test, y_train, y_test = train_test_split(
            features, target, test_size=0.3, shuffle=True, random_state=21
        )

        # Create the XGBoost DMatrix for our training and testing splits
        dtrain = xgb.DMatrix(X_train, y_train, enable_categorical=True)
        dtest = xgb.DMatrix(X_test, y_test, enable_categorical=True)

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
        bst = xgb.train(
            params, dtrain, num_boost_round=5,
            evals=[(dtrain, 'train')]
        )
        # save model
        bst.save_model(f'{path}xgboost_airflow.model')

        # save dtest DMatrix
        dtest.save_binary(f'{path}dtest.buffer')

        return bst


    @task()
    def predict():
        '''
        Use saved XGBoost model to make predictions.
  
        '''
        # load model and dtest
        bst2 = xgb.Booster(model_file=f'{path}xgboost_airflow.model')
        dtest2 = xgb.DMatrix(f'{path}dtest.buffer')

        # make predictions
        y_pred = bst2.predict(dtest2)

        # store predictions
        save(f'{path}test_ypred.npy', y_pred)

        # evaluate predictions
        mae = mean_absolute_error(y_test, y_pred)
        print(f"Mean Absolute Error: {mae}")
        return y_pred    

    # Call task functions in order
    subset = load_data()
    subset_clean = preprocess(subset)
    output = train_xgboost(subset_clean)


# Call taskflow
demo = train_xgboost()