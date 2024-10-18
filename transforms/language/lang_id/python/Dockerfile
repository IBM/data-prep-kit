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

# END OF STEPS destined for a data-prep-kit base image 

# set up environment required to install and use huggingface and fasttext
USER root
RUN apt update && apt install gcc g++ -y
RUN mkdir -p /home/dpk/.cache/huggingface/hub && chmod -R 777 /home/dpk/.cache/huggingface/hub
USER dpk

COPY --chown=dpk:root src/ src/
COPY --chown=dpk:root pyproject.toml pyproject.toml 
COPY --chown=dpk:root requirements.txt requirements.txt
RUN pip install --no-cache-dir -e .

# clean up apt
USER root 
RUN apt-get remove gcc g++ -y \
    && apt clean \
    && rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*
USER dpk

#COPY requirements.txt requirements.txt
#RUN pip install --no-cache-dir -r  requirements.txt

# copy the main() entry point to the image 
COPY ./src/lang_id_transform_python.py .

# copy some of the samples in
COPY ./src/lang_id_local.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/dpk

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
