FROM docker.io/rayproject/ray:2.9.3-py310

ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

# Copy and install data processing libraries
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=ray:users data-processing-lib-ray/ data-processing-lib-ray/
RUN cd data-processing-lib-ray    && pip install --no-cache-dir -e .

COPY --chown=ray:users python_apiserver_client python_apiserver_client/
RUN cd python_apiserver_client && pip install --no-cache-dir -e .

COPY --chown=ray:users workflow_support_lib workflow_support_lib/
RUN cd workflow_support_lib && pip install --no-cache-dir -e .
ENV KFP_v2=$KFP_v2
# remove credentials-containing file
RUN rm requirements.txt
# components
COPY ./src /pipelines/component/src
