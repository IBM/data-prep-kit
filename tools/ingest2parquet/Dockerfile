ARG BASE_IMAGE=docker.io/rayproject/ray:2.24.0-py310

FROM ${BASE_IMAGE}

# install pytest
RUN pip install --no-cache-dir pytest
ARG WHEEL_FILE_NAME

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-dist data-processing-dist
RUN  pip install data-processing-dist/${WHEEL_FILE_NAME}[ray]

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

RUN rm requirements.txt
# copy source
COPY --chown=ray:users ./src .
# copy test
COPY --chown=ray:users test/ test/
COPY --chown=ray:users test-data/ test-data/
# Set environment
ENV PYTHONPATH /home/ray
