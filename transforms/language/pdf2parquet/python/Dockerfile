FROM docker.io/python:3.10.14-slim-bullseye

RUN pip install --upgrade --no-cache-dir pip 

# install pytest
RUN pip install --no-cache-dir pytest

RUN \
    apt-get update \
    # for opencv, towhee
    && apt-get install -y libgl1 libglib2.0-0 curl wget \
    && apt-get clean

# Create a user and use it to run the transform
RUN useradd -ms /bin/bash dpk
USER dpk
WORKDIR /home/dpk

ARG PIP_INSTALL_EXTRA_ARGS

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=dpk:root data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .

# END OF STEPS destined for a data-prep-kit base image 

COPY --chown=dpk:root pyproject.toml pyproject.toml 
COPY --chown=dpk:root requirements.txt requirements.txt
RUN pip install ${PIP_INSTALL_EXTRA_ARGS} --no-cache-dir -e .

# Download models
RUN python -c 'from deepsearch_glm.utils.load_pretrained_models import load_pretrained_nlp_models; load_pretrained_nlp_models(verbose=True);'
RUN python -c 'from docling.document_converter import DocumentConverter; s=DocumentConverter.download_models_hf(); print(f"Models cached in {s}")'

# copy the main() entry point to the image
COPY --chown=dpk:root src/pdf2parquet_transform.py ./
COPY --chown=dpk:root src/pdf2parquet_transform_python.py ./

# copy some of the samples in
COPY --chown=dpk:root ./src/pdf2parquet_local.py local/


# copy test
COPY test/ test/
COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/dpk

# Parallelism
ENV OMP_NUM_THREADS=2

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
