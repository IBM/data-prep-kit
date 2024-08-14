# Setup a Local Python Dev Environment

## Step-1: Anaconda Python environment

You can install Anaconda by following the [guide here](https://www.anaconda.com/download/).

## Step-2: Create a custom environment

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

## Step-3: Create a Venv


```bash
## go to project dir (assumes repo name is 'data-prep-kit')
cd data-prep-kit/examples/notebooks/rag

make   clean
make   venv
```

This command will 

- create a python virtual environmnet in `venv` directory.
- install DPK modules
- any dependencies listed in `requirements.txt`

We only have to run this command once.

If you make any modifications to   `requirements.txt`  or DPK modules are updated, run this step again.

## Step-4: Test the created venv

```bash
## go to project dir (assumes repo name is 'data-prep-kit')
cd data-prep-kit/examples/notebooks/rag

# activate env
source   venv/bin/activate

## Check python version
python --version
# should say : 3.11
```

To deactivate

`deactivate`
