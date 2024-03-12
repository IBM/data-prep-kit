FROM docker.io/rayproject/ray:2.9.1-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

COPY  --chown=ray ./src /home/ray/transform
COPY  --chown=ray ./test-data /home/ray/test-data
ENV PYTHONPATH /home/ray