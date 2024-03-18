# Base image for KFP.

FROM python:3.10-slim-bullseye

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

RUN pip install --no-cache-dir -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple fm-data-processing-kfp==0.0.3.dev15

RUN pip install pydantic==2.6.4

COPY ./src /pipelines/component/src
