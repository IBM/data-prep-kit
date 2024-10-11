ARG BASE_IMAGE=docker.io/rayproject/ray:2.24.0-py310

FROM ${BASE_IMAGE}

RUN pip install --upgrade --no-cache-dir pip 

# install pytest
RUN pip install --no-cache-dir pytest

ARG PIP_INSTALL_EXTRA_ARGS

RUN \
    sudo apt-get update \
    # for opencv, towhee
    && sudo apt-get install -y libgl1 libglib2.0-0 curl wget \
    && sudo apt-get clean


# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=ray:users data-processing-lib-ray/ data-processing-lib-ray/ 
RUN cd data-processing-lib-ray    && pip install --no-cache-dir -e .
COPY --chown=ray:users python-transform/  python-transform/
RUN cd python-transform && pip install ${PIP_INSTALL_EXTRA_ARGS} --no-cache-dir -e .


COPY --chown=ray:users pyproject.toml pyproject.toml 
COPY --chown=ray:users requirements.txt requirements.txt
RUN pip install ${PIP_INSTALL_EXTRA_ARGS} --no-cache-dir -e .

# Download models
RUN python -c 'from deepsearch_glm.utils.load_pretrained_models import load_pretrained_nlp_models; load_pretrained_nlp_models(verbose=True);'
# RUN python -c 'from docling.document_converter import DocumentConverter; from pathlib import Path; DocumentConverter.download_models_hf(local_dir=Path("./artifacts/"));'
RUN python -c 'from docling.document_converter import DocumentConverter; s=DocumentConverter.download_models_hf(); print(f"Models cached in {s}")'

# copy the main() entry point to the image
COPY --chown=ray:users src/pdf2parquet_transform_ray.py ./

# copy some of the samples in
COPY --chown=ray:users ./src/pdf2parquet_local_ray.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/ray

# Parallelism
ENV OMP_NUM_THREADS=2

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
