FROM docker.io/rayproject/ray:2.9.3-py310

ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL data-prep-lab=${DPL_LIB_VERSION}
LABEL data-prep-lab-kfp=${DPL_LIB_KFP_VERSION}

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt



COPY ./src /pipelines/component/src
