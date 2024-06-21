FROM docker.io/python:3.10.14-slim-bullseye

RUN pip install --upgrade pip 

# install pytest
RUN pip install --no-cache-dir pytest

# Create a user and use it to run the transform
RUN useradd -ms /bin/bash dpk
USER dpk
WORKDIR /home/dpk

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=dpk:root data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .

# END OF STEPS destined for a data-prep-kit base image 

# set up environment required to install kenlm 
USER root
RUN apt update && apt install -y software-properties-common build-essential
USER dpk

COPY --chown=dpk:root src/ src/
COPY --chown=dpk:root pyproject.toml pyproject.toml 
RUN pip install --no-cache-dir -e .

# clean up apt
# USER root 
# RUN apt-get remove gcc g++ -y \
#     && apt clean \
#     && rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*
# USER dpk

# copy source data
COPY ./src/doc_quality_transform_python.py .
COPY ./src/doc_quality_local.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/
COPY ldnoobw/ ldnoobw/

# Set environment
ENV PYTHONPATH /home/dpk

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
