FROM docker.io/rayproject/ray:2.9.3-py310 AS base

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

USER root 
RUN sudo apt update && sudo apt install gcc g++ -y
USER ray 

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

USER root 
RUN sudo apt remove gcc g++ -y \
    && sudo apt clean \
    && sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*
USER ray 

COPY src/ /home/ray/

# Install code to enable testing on the image.
RUN pip install --no-cache-dir pytest
COPY test/ test/
COPY test-data/ test-data/

USER root
RUN chown -R ray /home/ray/test
RUN chown -R ray /home/ray/test-data
USER ray

ENV PYTHONPATH /home/ray
