FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

# Add packages needed to install kenlm
USER root
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get install -y build-essential
USER ray
RUN pip install --no-cache-dir https://github.com/kpu/kenlm/archive/master.zip

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

# Copy the model files into the "lm_sp" directory
RUN mkdir lm_sp
COPY lm_sp/ lm_sp/

COPY ldnoobw/ ldnoobw/

COPY ./src/doc_quality_transform.py .
COPY ./src/doc_quality_utils.py .
COPY ./src/cc_net_prepro.py .
COPY ./src/doc_c4_statistics.py .
COPY ./src/doc_Gopher_statistics.py .
COPY ./src/perplexity.py .

USER root
RUN apt remove build-essential -y
USER ray

ENV PYTHONPATH /home/ray
