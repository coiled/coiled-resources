# coiled-resources

Notebooks used in blogs and videos and open source datasets.

## Setting up your machine

You can install the dependencies on your local machine to run these notebooks by creating a conda environment:

```
conda env create -f envs/crt-003.yml
```

`crt` stands for [coiled-runtime](https://github.com/coiled/coiled-runtime), which pins a set of Dask runtime dependencies that are known to happily coexist.

Activate the environment with `conda activate crt-003`.

Open the project in your browser with `jupyter lab`.

To create a the same software environment in your Coiled account, run this command: `coiled env create -n crt-003 --conda envs/crt-003.yml`.

Here's how to create a cluster that uses the coiled-runtime software environment: `cluster = coiled.Cluster(name="powers-crt-003", software="crt-003", n_workers=5)`.

## Notebooks

Some of the notebooks are designed to run locally and others run on cloud machines via Coiled.

You can follow the [Coiled getting started](https://docs.coiled.io/user_guide/getting_started.html) guide to get your machine setup.  Coiled gives you some free credits, so you can easily try out the platform.

Some notebooks in this repo require conda environments with additional customization.  You can find `environment.yml` files to build those environments in the respective directories.

## Contributing

We welcome community contributions, especially [MCVE](https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports) analyses that others will find useful.

Feel free to create an issue and we'll be happy to brainstorm contributions.

