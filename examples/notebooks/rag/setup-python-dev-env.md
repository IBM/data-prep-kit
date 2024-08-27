# Setup a Local Python Dev Environment

## Step-1: Setup Python env

### Option A (Recommended): Anaconda Python environment

You can install Anaconda by following the [guide here](https://www.anaconda.com/download/).


We will create an environment for this workshop with all the required libraries installed.

**Make sure python version is 3.11**

```bash
conda create -n data-prep-kit-1 -y python=3.11

# activate the new conda environment
conda activate data-prep-kit-1
# make sure env is swithced to data-prep-kit-1

## Check python version
python --version
# should say : 3.11
```

Install dependencies

```bash
cd examples/notebooks/rag

pip  install  -r requirements.txt
```

### Option B: Python virtual env

Make sure you have python version 3.11 installed

```bash
## Check python version
python --version
# should say : 3.11
```

Create a venv

```bash
cd examples/notebooks/rag


python -m venv venv

## activate venv
source ./venv/bin/activate

## Install requirements
pip install -r requirements.txt
```

## Step-2: Launch Jupyter

`jupyter lab`

This will open a browser window.  Run the notebooks as indicated