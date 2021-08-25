import coiled
import dask
import dask.dataframe as dd
import folium
import streamlit as st
import os
import logging
import contextlib
from uuid import uuid4
from dask.distributed import Client
from folium.plugins import HeatMap
from streamlit_folium import folium_static


# Text in Streamlit
st.header("Coiled and Streamlit")
st.subheader("Analyzing Large Datasets with Coiled and Streamlit")
st.write(
    """
    The computations for this Streamlit app are powered by Coiled, which
    provides on-demand, hosted Dask clusters in the cloud. Change the options
    below to view different visualizations and summary statistics of the dataset,
    then let Coiled handle all of the infrastructure and heavy computation.

    """
)
st.subheader("About the Dataset")
st.write(
    """
    The dataset contains more than 145 million records (>10GB) of NYC taxi journeys in 2015. 

    Visualize locations of the first 500 pick-ups and drop-offs below using an interactive Folium map. 

    """
)


# Interactive widgets in Streamlit
taxi_mode = st.selectbox("Taxi pickup or dropoff?", ("Pickups", "Dropoffs"))
num_passengers = st.slider("Number of passengers", 0, 9, (0, 9))

# Start and connect to Coiled cluster
cluster_state = st.empty()


@st.cache(ttl=1200)
def generate_cluster_name():
    cluster_name = f"streamlit-{uuid4().hex[:5]}"
    return cluster_name

@st.cache(ttl=1200, allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def start_cluster():
    cluster_state.write("Starting or connecting to Coiled cluster...")
    dask.config.set({"coiled.token":st.secrets['token']})
    cluster_name = generate_cluster_name()
    logging.info(cluster_name)
    cluster = coiled.Cluster(
        n_workers=10,
        name=cluster_name,
        software="coiled-examples/streamlit",
    )
    return cluster

def attach_client():
    cluster = start_cluster()
    try:
        client = Client(cluster)
        client.wait_for_workers(5)   
        return client 
    except Exception as error:
        logging.exception(error)

client = attach_client()  #change name of this function


if not client or client.status == "closed":
    # In a long-running Streamlit app, the cluster could have shut down from idleness.
    # If so, clear the Streamlit cache to restart it.
    with contextlib.suppress(AttributeError):
        client.close()
    st.caching.clear_cache()
    client = attach_client()

cluster_state.write(f"Coiled cluster is up! ({client.dashboard_link})")

# Load data (runs on Coiled)
@st.cache(hash_funcs={dd.DataFrame: dask.base.tokenize})
def load_data():
    df = dd.read_csv(
        "s3://nyc-tlc/trip data/yellow_tripdata_2015-*.csv",
        usecols=[
            "passenger_count",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "tip_amount",
            "payment_type",
        ],
        storage_options={"anon": True},
        blocksize="16 MiB",
    )
    df = df.dropna()
    df.persist()
    return df


df = load_data()

# Filter data based on inputs (runs on Coiled)
with st.spinner("Calculating map data..."):
    map_data = df[
        (df["passenger_count"] >= num_passengers[0])
        & (df["passenger_count"] <= num_passengers[1])
    ]

    if taxi_mode == "Pickups":
        map_data = map_data.iloc[:, [2, 1]]
    elif taxi_mode == "Dropoffs":
        map_data = map_data.iloc[:, [4, 3]]

    map_data.columns = ["lat", "lon"]
    map_data = map_data.loc[~(map_data == 0).all(axis=1)]
    map_data = map_data.head(500)

# Display map in Streamlit
st.subheader("Map of selected rides")
m = folium.Map([40.76, -73.95], tiles="cartodbpositron", zoom_start=12)
HeatMap(map_data).add_to(folium.FeatureGroup(name="Heat Map").add_to(m))
folium_static(m)

# Performing a groupby
st.subheader(
    'Time for some heavier lifting!'
)

st.write(
    '''
    Let's move on to doing some heavier lifting to really see Dask in action.
    Below, you can group by a column and calculate a summary statistic for the tip amount. While the map above visualizes only the first 500 trips, this computation is done on the entire dataset containing more than 145 million rows.
    \n Select a column to group by below and a summary statistic to calculate:
    '''
)

# Interactive widgets in Streamlit
groupby_column = st.selectbox(
    "Which column do you want to group by?",
    ('passenger_count', 'payment_type')
)

aggregator = st.selectbox(
    "Which summary statistic do you want to calculate?",
    ("Mean", "Sum", "Median")
)

st.subheader(
    f"The {aggregator} tip_amount by {groupby_column} is:"
)

if st.button('Start Computation!'):
    with st.spinner("Performing your groupby aggregation..."):
        if aggregator == "Sum":
            st.write(
                df.groupby(groupby_column).tip_amount.sum().compute()
            )
        elif aggregator == "Mean":
            st.write(
                df.groupby(groupby_column).tip_amount.mean().compute()
            )
        elif aggregator == "Median":
            st.write(
                df.groupby(groupby_column).tip_amount.median().compute()
            )

# Option to scale cluster up/down
st.subheader(
    "Scaling your cluster up or down"
)

st.write(
    '''
    We spun our Coiled Cluster up with 10 workers.
    You can scale this number up or down using the slider and button below.
    \n Note that scaling a cluster up takes a couple of minutes.
    '''
)

num_workers = st.slider(
    "Number of workers",
    5,
    20,
    (10)
)

if st.button("Scale your cluster!"):
    coiled.Cluster(name='streamlit').scale(num_workers)


# Option to shutdown cluster
st.subheader(
    "Cluster Hygiene"
)

st.write(
    '''
    To avoid incurring unnecessary costs, click the button below to shut down your cluster.
    Note that this means that a new cluster will have to be spun up the next time you run the app.
    '''
)

if st.button('Shutdown Cluster'):
    st.write("This functionality is disabled for this public example to ensure a smooth experience for all users.")    
