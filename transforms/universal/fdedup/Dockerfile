FROM docker.io/rayproject/ray:2.9.3-py310

ARG DATA_PREP_LAB
ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL data-prep-lab=${DATA_PREP_LAB}

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt


COPY ./src/fdedup_transform.py .
COPY ./src/fdedup_support.py . 
# Japanese support
COPY ./src/text_normalizer.py . 
COPY ./src/ja.sp.model . 


# Install pytest so we can test the image later
RUN pip install --no-cache-dir pytest
COPY test/ test/
COPY test-data/ test-data/

USER root
RUN chown -R ray /home/ray/test
RUN chown -R ray /home/ray/test-data
USER ray

ENV PYTHONPATH /home/ray
