FROM docker.io/rayproject/ray:2.9.3-py310

# install pytest
RUN pip install --no-cache-dir pytest

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).

COPY --chown=ray:users data-processing-spark/ data-processing-spark/
RUN cd data-processing-spark && pip install --no-cache-dir -e .

# Install the base library 2nd to overwrite the data-prep-kit dependency from data-prep-kit-spark
COPY --chown=ray:users data-processing-lib/ data-processing-lib/
RUN cd data-processing-lib && pip install --no-cache-dir -e .

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

# copy source data
COPY ./src/noop_transform.py .
COPY ./src/noop_local.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# copy config
COPY config/ config/

# Set environment
ENV PYTHONPATH /home/ray

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
