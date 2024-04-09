FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY
ARG EXTRA_ARTIFACTORY_URL

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).
COPY data-processing-lib/ data-processing-lib/
USER root
RUN chown -R ray /home/ray/data-processing-lib
USER ray
RUN pip install --no-cache-dir --extra-index-url ${EXTRA_ARTIFACTORY_URL} --no-cache-dir -e data-processing-lib

COPY ./src/tokenization_transform.py .
COPY ./src/tokenization_utils.py .

# Install pytest so we can test the image later
RUN pip install --no-cache-dir pytest
COPY test/ test/
COPY test-data/ test-data/

USER root
RUN chown -R ray /home/ray/test
RUN chown -R ray /home/ray/test-data
USER ray

ENV PYTHONPATH /home/ray
