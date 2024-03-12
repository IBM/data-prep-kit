#FROM python:3.10.12-slim-bullseye
FROM clamav/clamav:latest AS clamav
FROM public.ecr.aws/docker/library/python:3.10.13-slim-bullseye AS base

USER root
RUN apt -y update && apt install -y clamav-daemon
COPY --link --chown=clamav:0 --chmod=774 --from=clamav /var/lib/clamav/ /var/lib/clamav/
COPY --chown=clamav:0 --chmod=774 ./docker/clamd.conf /etc/clamav/clamd.conf
RUN mkdir -p /var/run/clamav \
    && chown clamav:0 /var/run/clamav /var/log/clamav \
    && chmod -R 774 /var/run/clamav /var/log/clamav \
    && echo "TCPSocket 3310" >> /etc/clamav/clamd.conf

FROM base AS clamav-local

CMD ["/bin/bash", "-c", "freshclam && clamd --debug --foreground"]
EXPOSE 3310/tcp

FROM base AS antivirus

ENV TZ=US \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /T5
RUN apt -y update && apt install -y vim sudo bash curl unzip procps gcc

ENV PYTHONPATH "$PYTHONPATH:/T5"
ENV OPENBLAS_NUM_THREADS "1"
ENV PATH "/T5:$PATH"
#ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
#ENV JVM_PATH "/usr/lib/jvm/java-11-openjdk-amd64/lib/server/libjvm.so"
ENV HOME "/T5"

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir --upgrade setuptools

# read credentials to pull dependencies from artifactory
ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

# Setup our python requirements w/o watson_nlp since that is in the base image
COPY requirements.txt /T5/requirements.txt
RUN pip install --no-cache-dir -r  /T5/requirements.txt
# remove credentials-containing file
RUN rm /T5/requirements.txt 

# After all pip installs are done, we can remove pip's cache which can be considerable for large depenences.
# Do this before the chmod/chgrp stuff below
RUN rm -rf /T5/.cache/pip

# GCC only needed while installing some python modules
RUN apt remove -y gcc

# Expect the models to be avavilable in the host via "python preload.py" so that we don't trigger
# watson_nlp's use of ARTIFACTORY env vars.

# Copy test source , but avoid copying any models (entity_*), we already did that above
COPY ./src/ /T5/

# Do this because when running in the cluster, we may not be running as root.
RUN chmod a+r+w+x -R /T5

WORKDIR /T5
RUN freshclam
CMD ["/bin/bash", "-c", "clamd --foreground"]