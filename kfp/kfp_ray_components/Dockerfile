# Base image for KFP.

FROM python:3.10-slim-bullseye

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


COPY ./src /pipelines/component/src
