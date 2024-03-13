FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

COPY ./src/fdedup_transform.py .
COPY ./src/fdedup_support.py . 
# Japanese support
COPY ./src/text_normalizer.py . 
COPY ./src/ja.sp.model . 

ENV PYTHONPATH /home/ray

