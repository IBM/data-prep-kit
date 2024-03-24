# Base image for KFP.

FROM python:3.10-slim-bullseye

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY
ARG FM_DATA_PROCESSING
ARG FM_DATA_PROCESSING_KFP
ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL fm-data-processing=${FM_DATA_PROCESSING}
LABEL fm-data-processing-kfp=${FM_DATA_PROCESSING_KFP}

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

RUN pip install --no-cache-dir -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple fm-data-processing-kfp==0.0.3.dev22

RUN pip install pydantic==2.6.3
RUN pip install pyiceberg==0.5.1

COPY ./src /pipelines/component/src
