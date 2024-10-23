ARG BASE_IMAGE=quay.io/dataprep1/data-prep-kit/data-prep-kit-spark-3.5.2:latest
FROM ${BASE_IMAGE}

USER root
# install pytest
RUN pip install --no-cache-dir pytest

WORKDIR ${SPARK_HOME}/work-dir
ARG WHEEL_FILE_NAME

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-dist data-processing-dist
RUN  pip install data-processing-dist/${WHEEL_FILE_NAME}[spark]


# Install project source
COPY --chown=spark:root src/ src/
COPY --chown=spark:root pyproject.toml pyproject.toml 
RUN pip install --no-cache-dir -e .

# copy the main() entry point to the image 
COPY ./src/doc_id_transform_spark.py .

# copy some of the samples in
COPY src/doc_id_local_spark.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

USER spark

# Set environment
ENV PYTHONPATH=${SPARK_HOME}/work-dir/:${PYTHONPATH}

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
