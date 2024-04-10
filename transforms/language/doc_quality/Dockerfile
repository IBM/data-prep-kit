FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY
ARG EXTRA_ARTIFACTORY_URL


# Add packages needed to install kenlm
USER root
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get install -y build-essential
USER ray
RUN pip install --no-cache-dir https://github.com/kpu/kenlm/archive/master.zip

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).
COPY data-processing-lib/ data-processing-lib/
USER root
RUN chown -R ray /home/ray/data-processing-lib
USER ray
RUN pip install --no-cache-dir --extra-index-url ${EXTRA_ARTIFACTORY_URL} --no-cache-dir -e data-processing-lib

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

COPY ./src/doc_quality_transform.py .
COPY ./src/doc_quality_utils.py .
COPY ./src/cc_net_prepro.py .
COPY ./src/doc_c4_statistics.py .
COPY ./src/doc_Gopher_statistics.py .
COPY ./src/perplexity.py .

# Install pytest so we can test the image later
RUN pip install --no-cache-dir pytest
COPY test/ test/
COPY test-data/ test-data/
# Copy the model files into the "lm_sp" directory
COPY lm_sp/  lm_sp/
COPY ldnoobw/ ldnoobw/

USER root
RUN apt remove build-essential -y
RUN chown -R ray /home/ray/test
RUN chown -R ray /home/ray/test-data
USER ray

ENV PYTHONPATH /home/ray