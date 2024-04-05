FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

USER root

RUN apt update && \
    apt install cmake -y

USER ray

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

# Copy the model files into the "lm_sp" directory
COPY cc_net/data/lm_sp/* /lm_sp

COPY ldnoobw/ ldnoobw

COPY ./src/doc_quality_transform.py .
COPY ./src/doc_quality_utils.py .
COPY ./src/cc_net_prepro.py .
COPY ./src/doc_c4_statistics.py .
COPY ./src/doc_Gopher_statistics.py .
COPY ./src/perplexity.py .
ENV PYTHONPATH /home/ray
