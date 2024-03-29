FROM docker.io/rayproject/ray:2.9.3-py310 AS base
USER root
RUN apt -y update \
    && apt install -y clamav-daemon \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER ray

FROM docker.io/clamav/clamav:latest AS clamav
FROM base AS clamav-local
USER root
COPY --chown=ray:users --from=clamav /var/lib/clamav/ /var/lib/clamav/
COPY --chown=ray:users clamd.conf /etc/clamav/clamd.conf
RUN freshclam \
    && mkdir -p /var/run/clamav \
    && chown -R ray:users /var/run/clamav /var/log/clamav /var/lib/clamav
USER ray
CMD ["/bin/bash", "-c", "clamd --debug --foreground"]

FROM base AS antivirus

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY --from=clamav-local --chown=ray:users /var/lib/clamav/ /var/lib/clamav/
COPY --from=clamav-local --chown=ray:users /etc/clamav/clamd.conf /etc/clamav/clamd.conf
COPY --from=clamav-local --chown=ray:users /var/log/clamav/clamav.log /var/log/clamav/clamav.log
COPY --from=clamav-local --chown=ray:users /var/run/clamav /var/run/clamav
COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf requirements.txt .cache/pip \
    && echo "clamd" >> .profile

COPY src/*_transform.py  ./
ENV PYTHONPATH /home/ray
