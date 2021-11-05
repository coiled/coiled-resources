# Dask Live by Coiled Tutorial

The purpose of this tutorial is to introduce folks to Dask and show them how to scale their python data-science and machine learning workflows. The materials covered are:

0. Overview of dask - How it works and when to use it. 
1. Dask Delayed: How to parallelize existing Python code and your custom algorithms. 
2. Schedulers: Single Machine vs Distributed, and the Dashboard.   
3. From pandas to Dask: How to manipulate bigger-than-memory DataFrames using Dask.  
4. Dask-ML: Scalable machine learning using Dask.  

## Prerequisites

To follow along and get the most out of this tutorial it would help if you Know:

- Programming fundamentals in Python (e.g variables, data structures, for loops, etc).
- A bit of or are familiarized with `numpy`, `pandas` and `scikit-learn`. 
- Jupyter Lab/ Jupyter Notebooks
- Your way around the shell/terminal 

However, the most important prerequisite is being willing to learn, and everyone is 
welcomed to tag along and enjoy the ride. If you would like to watch and not code along, 
not a problem.

## Get set up

We have two options for you to follow this tutorial:

1. Click on the binder button right below, this will spin up the necessary computational environment for you so you can write and execute the notebooks directly on the browser. Binder is a free service so resources are not guaranteed, but they usually work. One thing 
to keep in mind is that the amount of resources are limited and sometimes you won't be able to see the benefits of parallelism due to this limitation. 

    *IMPORTANT*: If you are joining the live session, make sure to click on the button few minutes before we start so we are ready to go. 

    [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/coiled/dask-mini-tutorial/HEAD)


2. You can create your own set-up locally. To do this you need to be comfortable with the git and github as well as installing packages and creating software environments. If so, follow the next steps:

    *IMPORTANT:* If you are joining for a live session please make sure you do the setup in advance, and be ready to go once the session starts.

    1. **Clone this repository**
        In your terminal:

        ```
        git clone https://github.com/coiled/dask-mini-tutorial.git
        ```
        Alternatively, you can download the zip file of the repository at the top of the main page of the repository. This is a good option if you don't have experience with git.
        
    2. Download Anaconda 
        If you do not have anaconda already install, you will need the Python 3 [Anaconda Distribution](https://www.anaconda.com/products/individual). If you don't want to install anaconda you can install all the packages with `pip`, if you take this route you will need to install `graphviz` separately before installing `pygraphviz`.
    
    3. Create a conda environment
        In your terminal navigate to the directory where you have cloned/downloaded th `dask-mini-tutorial` repo and install the required packages by doing:

        ```
        conda env create -f binder/environment.yml
        ```

        This will create a new environment called `dask-mini-tutorial`. To activate the environment do:

        ```
        conda activate dask-mini-tutorial
        ```

    4. Open Jupyter Lab
        Once your environment has been activated and you are in the `dask-mini-tutorial` repository, in your terminal do:

        ```
        jupyter lab
        ```

        You will see a notebooks directory, click on there and you will be ready to go.








