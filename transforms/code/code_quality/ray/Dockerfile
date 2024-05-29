FROM docker.io/rayproject/ray:2.9.3-py310

# install pytest
RUN pip install --no-cache-dir pytest

# Copy and install data processing libraries - ray first to allow installation of latest python source
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib-ray/ data-processing-lib-ray/ 
RUN cd data-processing-lib-ray    && pip install --no-cache-dir -e .
COPY --chown=ray:users data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

# copy source data
COPY src/code_quality_transform.py .
COPY src/code_quality_local_ray.py local/

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

