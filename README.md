# coiled-resources

Notebooks used in blogs and videos and open source datasets.

## Setting up your machine

You can install the dependencies on your local machine to run these notebooks by creating a conda environment:

```
conda env create -f envs/standard-coiled.yml
```

Activate the environment with `conda activate standard-coiled`.

Open the project in your browser with `jupyter lab`.

## Notebooks

Some of the notebooks are designed to run locally and others run on cloud machines via Coiled.

You can follow the [Coiled getting started](https://docs.coiled.io/user_guide/getting_started.html) guide to get your machine setup.  Coiled gives you some free credits, so you can easily try out the platform.

Some notebooks in this repo require conda environments with additional customization.  You can find `environment.yml` files to build those environments in the respective directories.

## Datasets

Coiled hosts several public datasets in AWS S3 that you can easily query when experimenting with Dask.

Here's an example code snippet that creates a DataFrame with 662 million rows of data:

```python

ddf = dd.read_parquet(
    "s3://coiled-datasets/timeseries/20-years/parquet",
    storage_options={"anon": True, "use_ssl": True}
)
```

These easily accessible datasets make it a lot easier for you to run Dask analyses and perform benchmarking analyses.

## Contributing

We welcome community contributions, especially [MCVE](https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports) analyses that others will find useful.

Feel free to create an issue and we'll be happy to brainstorm contributions.

