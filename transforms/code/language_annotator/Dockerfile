FROM docker.io/rayproject/ray:2.9.3-py310

# install pytest
RUN pip install --no-cache-dir pytest

# Copy in the data processing framework source/project and install it
# This is expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib/ data-processing-lib/
# install data processing
RUN cd data-processing-lib && pip install --no-cache-dir -e .

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

# copy source data
COPY src/language_annotator_transform.py .
COPY src/language_annotator_local_ray.py local/

# copy test
COPY test/ test/
COPY test-data/ test-data/

# Set environment
ENV PYTHONPATH /home/ray
