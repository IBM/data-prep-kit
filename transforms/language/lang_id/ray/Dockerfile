ARG BASE_IMAGE=docker.io/rayproject/ray:2.24.0-py310

FROM ${BASE_IMAGE}

RUN pip install --upgrade --no-cache-dir pip 

# install pytest
RUN pip install --no-cache-dir pytest

# set up environment required to install and use huggingface and fasttext
USER root
RUN sudo apt update && sudo apt install gcc g++ -y
RUN mkdir -p /home/ray/.cache/huggingface/hub && chmod -R 777 /home/ray/.cache/huggingface/hub
USER ray

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=ray:users data-processing-lib-ray/ data-processing-lib-ray/ 
RUN cd data-processing-lib-ray    && pip install --no-cache-dir -e .
COPY --chown=ray:users python-transform/  python-transform/
RUN cd python-transform && pip install --no-cache-dir -e .

COPY --chown=ray:users src/ src/
COPY --chown=ray:users pyproject.toml pyproject.toml 
RUN pip install --no-cache-dir -e .

# clean up apt
USER root 
RUN sudo apt remove gcc g++ -y \
    && sudo apt clean \
    && sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*
USER ray

# copy the main() entry point to the image 
COPY ./src/lang_id_transform_ray.py .

# copy some of the samples in
COPY ./src/lang_id_local_ray.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/ray

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
