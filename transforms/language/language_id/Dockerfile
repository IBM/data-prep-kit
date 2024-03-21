FROM docker.io/rayproject/ray:2.9.3-py310 AS base

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt

RUN sudo apt update && sudo apt install gcc g++ -y
RUN pip install --no-cache-dir -r requirements.txt
RUN sudo apt remove gcc g++ -y \
    && sudo apt clean \
    && sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*

COPY src/ /home/ray/
ENV PYTHONPATH /home/ray