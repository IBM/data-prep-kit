FROM docker.io/python:3.10.14-slim-bullseye

RUN pip install --upgrade --no-cache-dir pip 

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

COPY --chown=dpk:root src/ src/
COPY --chown=dpk:root pyproject.toml pyproject.toml
COPY --chown=dpk:root README.md README.md
COPY --chown=dpk:root requirements.txt requirements.txt

RUN pip install --no-cache-dir -e .

# copy the main() entry point to the image 
COPY ./src/code_profiler_transform_python.py .

# copy some of the samples in
COPY ./src/code_profiler_local.py local/

# Copy the tree-sitter bindings (this is the important part)
COPY --chown=ray:users ../../input/tree-sitter-bindings/ /home/dpk/input/tree-sitter-bindings/

# copy test
# COPY test/ test/
# COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/dpk

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
