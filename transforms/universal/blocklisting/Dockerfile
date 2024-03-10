#FROM python:3.10.12-slim-bullseye
FROM public.ecr.aws/docker/library/python:3.10.13-slim-bullseye

ENV TZ=US \
    DEBIAN_FRONTEND=noninteractive
USER root

WORKDIR /T5
RUN apt -y update && apt install -y vim sudo bash curl unzip procps 
# For psutils and since some requirements seem to need gcc
RUN apt -y update && apt install -y gcc 

ENV PYTHONPATH "$PYTHONPATH:/T5"
ENV OPENBLAS_NUM_THREADS "1"
ENV PATH "/T5:$PATH"
ENV HOME "/T5"

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir --upgrade setuptools 

# read credentials to pull dependencies from artifactory
ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

# Setup our python requirements w/o watson_nlp since that is in the base image
COPY requirements.txt /T5/requirements.txt
RUN pip install --no-cache-dir -r  /T5/requirements.txt
# remove credentials-containing file
RUN rm /T5/requirements.txt 

# Remove gcc, no longer needed after pip install
RUN apt remove -y gcc && apt clean -y && apt autoremove -y

# After all pip installs are done, we can remove pip's cache which can be considerable for large depenences.
# Do this before the chmod/chgrp stuff below
RUN rm -rf /T5/.cache/pip


# Expect the models to be avavilable in the host via "python preload.py" so that we don't trigger
# watson_nlp's use of ARTIFACTORY env vars.

# Copy test source , but avoid copying any models (entity_*), we already did that above
COPY ./src/ /T5/

# Do this because when running in the cluster, we may not be running as root.
#RUN chgrp -R 0 /T5 && chmod -R g=u /T5
#RUN chmod +x /T5/*.sh
RUN chmod a+r+w+x -R /T5

WORKDIR /T5
CMD ["/bin/bash"]
