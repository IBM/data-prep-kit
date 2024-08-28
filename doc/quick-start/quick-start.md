# Quick Start for Data Prep Kit 
Here we provided short examples of various uses of the Data Prep Kit. Most users who want to jump right in can use standard pip install to deploy the data-prep-kit and the python or ray transforms to their virtual python environment. 

- When setting up a virtual environment it is recommended to use python3.11 as in the example below using conda. 


    - setup a virtual environment (example using conda)
 \
        `conda create -n data-prep-kit-1 -y python=3.11`
 

    - Install the gcc/g++ that is required while building fastext:
\
        `conda install gcc_linux-64`
\
        `conda install gxx_linux-64`


    - activate the new conda environment
\
        `conda activate data-prep-kit-1`


    - make sure env is switched to data-prep-kit-1 and Check python version.
\
        `python --version`
\
        The command above should say: 3.11


    - install jupyter lab
\
        `pip3 install jupyterlab`
 
then
- Deploy the latest release of the data prep toolkit library

    `pip3 install data-prep-toolkit`

    or 
-  Deploy the latest releases of the data prep toolkit library and all python transforms

    `pip3 install data-prep-toolkit-transforms`

    or 
-  Deploy the latest releases of the data prep toolkit library, all python transforms and all ray transforms

    `pip3 install data-prep-toolkit-transforms-ray`



## Running transforms 

* Notebooks
    * [Example data processing pipelines](../../examples/notebooks/README.md) - Use these to quickly process your data. A notebook structure allows a user to select/de-select transforms and change the order of processing as desired. 
* Command line  
    * [Using a docker image](run-transform-image.md) - runs a transform in a docker transform image 
    * [Using a virtual environment](run-transform-venv.md) - runs a transform on the local host 
    
## Creating transforms

* [Outside of the repository](new-transform-outside.md) - shows how to use pypi dependencies to create a transform independent of this repository.
* [Adding to this repository](new-transform-inside.md) - shows how to add a transform to this repository, including repository conventions and ci/cd. 

