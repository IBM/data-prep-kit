FROM docker.io/rayproject/ray:2.9.1-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

COPY ./src/fdedup_implementation.py transformer_launcher.py
COPY ./src/fdedup_support.py fdedup_support.py
# Japaneze support
COPY ./src/text_normalizer.py text_normalizer.py
COPY ./src/ja.sp.model ja.sp.model

ENV PYTHONPATH /home/ray

