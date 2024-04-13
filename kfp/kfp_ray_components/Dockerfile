# Base image for KFP.

FROM python:3.10-slim-bullseye

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY
ARG DATA_PREP_LAB
ARG DATA_PREP_LAB_KFP
ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL data-prep-lab=${DATA_PREP_LAB}
LABEL data-prep-lab-kfp=${DATA_PREP_LAB_KFP}

RUN pip install --no-cache-dir scipy==1.13.0
RUN pip install --no-cache-dir -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple data-prep-lab==${DATA_PREP_LAB}
RUN pip install --no-cache-dir -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple data-prep-lab-kfp==${DATA_PREP_LAB_KFP}

RUN pip install pydantic==2.6.3
RUN pip install pyiceberg==0.5.1

COPY ./src /pipelines/component/src
