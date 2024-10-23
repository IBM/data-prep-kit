ARG BASE_IMAGE=docker.io/rayproject/ray:2.36.1-py312

FROM ${BASE_IMAGE}

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

ARG WHEEL_FILE_NAME

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-dist data-processing-dist
RUN  pip install data-processing-dist/${WHEEL_FILE_NAME}[ray]

COPY --chown=ray:users shared_workflow_support_lib shared_workflow_support_lib/
RUN cd shared_workflow_support_lib && pip install --no-cache-dir -e .

COPY --chown=ray:users workflow_support_lib workflow_support_lib/
RUN cd workflow_support_lib && pip install --no-cache-dir -e .

# overwriting the installation of old versions of pydantic
RUN pip install --no-cache-dir pydantic==2.6.3

# remove credentials-containing file
RUN rm requirements.txt
# components
COPY ./src /pipelines/component/src

# Set environment
ENV KFP_v2=$KFP_v2

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
