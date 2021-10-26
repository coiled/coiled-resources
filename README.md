# coiled-resources

The notebooks in this repository are used in Coiled blogs, videos, and open datasets.

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

## coiled-datasets

Coiled hosts several public datasets in AWS S3 that you can easily query when experimenting with Dask.

Find the list of datasets and learn to access them here: [coiled-datasets](./coiled-datasets.md)

## Contributing

We welcome community contributions, especially [MCVE](https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports) analyses that others will find useful.

Feel free to create an issue and we'll be happy to brainstorm contributions.

