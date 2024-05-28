# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

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
COPY ./src/noop_transform.py .
COPY ./src/noop_local.py local/

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
