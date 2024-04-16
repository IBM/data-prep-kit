FROM docker.io/rayproject/ray:2.9.3-py310

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

RUN rm requirements.txt
# copy source
COPY ./src .
# copy test
COPY test/ test/
COPY test-data/ test-data/
# install pytest
RUN pip install --no-cache-dir pytest
# Set environment
ENV PYTHONPATH /home/ray
