ARG BASE_IMAGE=docker.io/rayproject/ray:2.24.0-py310

FROM ${BASE_IMAGE}

RUN pip install --upgrade --no-cache-dir pip 

# install pytest
RUN pip install --no-cache-dir pytest

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=ray:users data-processing-lib-ray/ data-processing-lib-ray/ 
RUN cd data-processing-lib-ray    && pip install --no-cache-dir -e .
COPY --chown=ray:users python-transform/  python-transform/
RUN cd python-transform && pip install --no-cache-dir -e .

# END OF STEPS destined for a data-prep-kit base image 

COPY --chown=ray:users pyproject.toml pyproject.toml 
COPY --chown=ray:users requirements.txt requirements.txt
RUN pip install --no-cache-dir -e .

# copy transform main() entry point to the image 
COPY --chown=ray:users src/html2parquet_transform_ray.py ./

# copy some of the samples in
COPY --chown=ray:users ./src/html2parquet_local_ray.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/ray

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
