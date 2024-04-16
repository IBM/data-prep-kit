FROM docker.io/rayproject/ray:2.9.3-py310

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt
RUN rm requirements.txt

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).
COPY data-processing-lib/ data-processing-lib/
USER root
RUN chown -R ray /home/ray/data-processing-lib
USER ray
# copy source data
COPY ./src/noop_transform.py .
COPY ./src/noop_local_ray.py local/
# copy test
COPY test/ test/
COPY test-data/ test-data/
# install pytest
RUN pip install --no-cache-dir pytest
# install data processing
RUN cd data-processing-lib && pip install --no-cache-dir -e .
# Set environment
ENV PYTHONPATH /home/ray

