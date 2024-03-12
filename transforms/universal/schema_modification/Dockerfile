FROM docker.io/rayproject/ray:2.9.1-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

COPY ./src/*_transform.py .
ENV PYTHONPATH /home/ray

