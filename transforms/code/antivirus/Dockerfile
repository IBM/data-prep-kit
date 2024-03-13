FROM docker.io/rayproject/ray:2.9.1-py310 AS base
USER root
RUN apt -y update \
    && apt install -y clamav-daemon \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER 1000

FROM clamav/clamav:latest AS clamav
FROM base AS clamav-local
USER root
COPY --link --chown=1000:100 --chmod=774 --from=clamav /var/lib/clamav/ /var/lib/clamav/
COPY --chown=1000:100 --chmod=774 ./docker/clamd.conf /etc/clamav/clamd.conf
RUN freshclam \
    && mkdir -p /var/run/clamav \
    && chown -R 1000:100 /var/run/clamav /var/log/clamav /var/lib/clamav
USER 1000
CMD ["/bin/bash", "-c", "freshclam && clamd --debug --foreground"]

FROM base AS antivirus

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_API_KEY

COPY --from=clamav-local --chown=1000:100 /var/lib/clamav/ /var/lib/clamav/
COPY --from=clamav-local --chown=1000:100 /etc/clamav/clamd.conf /etc/clamav/clamd.conf
COPY --from=clamav-local --chown=1000:100 /var/log/clamav/clamav.log /var/log/clamav/clamav.log
COPY --from=clamav-local --chown=1000:100 /var/run/clamav /var/run/clamav
COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf requirements.txt .cache/pip

COPY src/*_transform.py  ./
ENV PYTHONPATH /home/ray

CMD ["/bin/bash", "-c", "clamd --foreground"]