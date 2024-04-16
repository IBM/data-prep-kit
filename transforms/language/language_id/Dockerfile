FROM docker.io/rayproject/ray:2.9.3-py310 AS base

USER root 
RUN sudo apt update && sudo apt install gcc g++ -y
USER ray 

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib/ data-processing-lib/
# install data processing
RUN cd data-processing-lib && pip install --no-cache-dir -e .
# remove gcc
USER root 
RUN sudo apt remove gcc g++ -y \
    && sudo apt clean \
    && sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*
USER ray
# copy source
COPY ./src/lang_id_transform.py .
COPY ./src/lang_models.py .
COPY ./src/nlp.py .
COPY src/lang_id_local.py local/
# Install pytest so we can test the image later
RUN pip install --no-cache-dir pytest
COPY test/ test/
COPY test-data/ test-data/

ENV PYTHONPATH /home/ray
