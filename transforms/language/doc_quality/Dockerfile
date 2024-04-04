FROM docker.io/rayproject/ray:2.9.3-py310

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
RUN pip install --no-cache-dir https://github.com/kpu/kenlm/archive/master.zip
# remove credentials-containing file
RUN rm requirements.txt

# Copy the model files into the "lm_sp" directory
COPY /cc_net/data/lm_sp/en* /lm_sp

# Optionally, remove the "cc_net" folder if needed
RUN rm -rf /cc_net

COPY ./src/docquality_transform.py .
COPY ./src/docquality_utils.py .
ENV PYTHONPATH /home/ray
