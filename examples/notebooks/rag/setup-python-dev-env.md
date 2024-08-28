# Setup a Local Python Dev Environment

We can use 

- Option-A: Use Anaconda environment
- Option-B: Use python virtual env

Just follow one.  (A) is recommended!

## Option A (Recommended): Anaconda Python environment

You can install Anaconda by following the [guide here](https://www.anaconda.com/download/).


We will create an environment for this workshop with all the required libraries installed.

### A-1: Setup a conda env

```bash
conda create -n data-prep-kit-1 -y python=3.11

# activate the new conda environment
conda activate data-prep-kit-1
# make sure env is swithced to data-prep-kit-1

## Check python version
python --version
# should say : 3.11
```

### A-2: Install dependencies

```bash
cd examples/notebooks/rag

pip  install  -r requirements.txt
```

If any issues see [troubleshooting tips](#troubleshooting-tips)

### A-3: Start Jupyter

`jupyter lab`

This will usually open a browser window/tab.  We will use this to run the notebooks


## Option B: Python virtual env

### B-1: Have python version 3.11

```bash
## Check python version
python --version
# should say : 3.11
```

### B-2: Create a venv

```bash
cd examples/notebooks/rag


python -m venv venv

## activate venv
source ./venv/bin/activate

## Install requirements
pip install -r requirements.txt
```

If any issues see [troubleshooting tips](#troubleshooting-tips)


### B-3: Launch Jupyter

`./venv/bin/jupyter lab`

This will usually open a browser window/tab.  We will use this to run the notebooks

**Note:**: Make sure to run `./venv/bin/jupyter lab`, so it can load installed dependencies correctly.

## Troubleshooting Tips

### fasttext compile issue with GCC/G++ compiler version 13

`pip install` may fail because one of the python dependencies, `fasttext==0.9.2` compiles with GCC/G++ version 11, not version 13.

Here is how to fix this error:

```bash
## These instructions are for Ubuntu 22.04 and later

sudo apt update

## install GCC/G++ compilers version 11 
sudo apt install -y gcc-11  g++-11

## Verify installation
gcc-11  --version
g++-11  --version
# should say 11

## Set the compiler before doing pip install
CC=gcc-11  pip install -r requirements.txt 
```