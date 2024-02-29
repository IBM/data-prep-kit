FROM docker.io/rayproject/ray:2.9.1-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
# remove credentials-containing file
RUN rm requirements.txt

COPY ./src/ededup_implementation.py transformer_launcher.py
ENV PYTHONPATH /home/ray

