# Base image for KFP.

FROM python:3.10-slim-bullseye

ARG DATA_PREP_LAB
ARG DATA_PREP_LAB_KFP
ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL data-prep-lab=${DATA_PREP_LAB}
LABEL data-prep-lab-kfp=${DATA_PREP_LAB_KFP}

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt


COPY ./src /pipelines/component/src
