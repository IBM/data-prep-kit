FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
RUN pip install --no-cache-dir https://github.com/kpu/kenlm/archive/master.zip
# remove credentials-containing file
RUN rm requirements.txt

COPY ./src/docquality_transform.py .
ENV PYTHONPATH /home/ray
