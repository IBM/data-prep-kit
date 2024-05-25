ARG BASE_IMAGE=quay.io/dataprep1/data-prep-kit/data-prep-kit-spark-3.5.1:0.1.0
FROM ${BASE_IMAGE}

USER root
# install pytest
RUN pip install --no-cache-dir pytest

WORKDIR ${SPARK_HOME}/work-dir

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).

COPY --chown=spark:root data-processing-spark/ data-processing-spark/
RUN cd data-processing-spark && pip install --no-cache-dir -e .

# Install the base library 2nd to overwrite the data-prep-kit dependency from data-prep-kit-spark
COPY --chown=spark:root data-processing-lib/ data-processing-lib/
RUN cd data-processing-lib && pip install --no-cache-dir -e .

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy source data
COPY ./src/filter_transform.py .
COPY ./src/filter_local.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# copy config
COPY config/ config/

USER spark

# Set environment
ENV PYTHONPATH=${SPARK_HOME}/work-dir/:${PYTHONPATH}

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
