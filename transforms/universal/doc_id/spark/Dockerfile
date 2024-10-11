ARG BASE_IMAGE=quay.io/dataprep1/data-prep-kit/data-prep-kit-spark-3.5.2:latest
FROM ${BASE_IMAGE}

USER root
# install pytest
RUN pip install --no-cache-dir pytest

WORKDIR ${SPARK_HOME}/work-dir

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).

COPY --chown=spark:root data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=spark:root data-processing-lib-spark/ data-processing-lib-spark/
RUN cd data-processing-lib-spark && pip install --no-cache-dir -e .

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
