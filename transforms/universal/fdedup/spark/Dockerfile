ARG BASE_IMAGE=data-prep-kit-spark-3.5.2:0.3.0

FROM ${BASE_IMAGE}

# USER root
# install pytest
RUN pip install --no-cache-dir pytest

WORKDIR ${SPARK_HOME}/work-dir

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).
COPY --chown=spark:root data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=spark:root data-processing-lib-spark/ data-processing-lib-spark/
RUN cd data-processing-lib-spark && pip install --no-cache-dir -e .
COPY --chown=spark:root python-transform/  python-transform/
RUN cd python-transform && pip install --no-cache-dir -e .

# Install project source
COPY --chown=spark:root src/ src/
COPY --chown=spark:root pyproject.toml pyproject.toml
RUN mkdir -p /opt/spark/work-dir/src/templates && \
    mkdir -p /opt/spark/work-dir/config

# install requirements from requirements.txt
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY deployment/kubernetes/spark-executor-pod-template.yml /opt/spark/work-dir/src/templates/
COPY deployment/kubernetes/spark_profile.yml /opt/spark/work-dir/config/

RUN pip install --no-cache-dir -e .

# copy the main() entry point to the image
COPY ./src/signature_calc_spark.py .

# copy some of the samples in
COPY src/signature_calc_transform_spark.py fdedup_transform_spark.py
COPY src/signature_calc_spark.py local/fdedup_local_spark.py

# copy test
COPY test/ test/
COPY test-data/ test-data/

USER spark

# Set environment
ENV PYTHONPATH=${SPARK_HOME}/work-dir/:${SPARK_HOME}/work-dir/src/:${PYTHONPATH}
ENV PATH=${SPARK_HOME}/work-dir/.local/bin/:${PATH}

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
