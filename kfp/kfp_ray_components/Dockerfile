FROM docker.io/rayproject/ray:2.9.3-py310

ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT

# install libraries
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

# install data processing and kfp support libs
# Copy in the frameworks source/project and install them
# This is expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib/ data-processing-lib/
RUN cd data-processing-lib && pip install --no-cache-dir -e .
COPY --chown=ray:users kfp_support_lib/ kfp_support_lib/
RUN cd kfp_support_lib && pip install --no-cache-dir -e .
# remove credentials-containing file
RUN rm requirements.txt
RUN rm kfp_support_lib/
RUN rm data-processing-lib/



COPY ./src /pipelines/component/src
