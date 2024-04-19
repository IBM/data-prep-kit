FROM docker.io/rayproject/ray:2.9.3-py310

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r  requirements.txt

RUN rm requirements.txt
# copy source
COPY --chown=ray:users ./src .
# copy test
COPY --chown=ray:users test/ test/
COPY --chown=ray:users test-data/ test-data/
# install pytest
RUN pip install --no-cache-dir pytest
# Set environment
ENV PYTHONPATH /home/ray
