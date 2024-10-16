# see https://github.com/apache/spark-docker/blob/master/3.5.1/scala2.12-java17-ubuntu/Dockerfile
# and https://github.com/apache/spark-docker/blob/master/3.5.1/scala2.12-java17-python3-ubuntu/Dockerfile
# The reasons we are modifying Apache dockerfile are:
#   1. We support cofiguration of the Python version
#   2. We are setting non root user

# use the latest open JRE 17 minimal as the build image
ARG OPENJRE_TAG=17-jre-ubi9-minimal
FROM eclipse-temurin:${OPENJRE_TAG} as build

# download requested Spark distribution and extract into /opt/spark
ARG SPARK_VERSION=3.5.2
ARG HADOOP_MAJOR_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION} /opt/spark

FROM python:3.10-slim

# install additional packages required by spark
RUN set -ex && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt install -y bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* && rm -rf /var/lib/apt/lists/*

# Copy JRE from the Open JRE build image
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=build $JAVA_HOME $JAVA_HOME
# add java to executable path
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set /opt/spark as SPARK_HOME and copy Spark files there from the build image
ENV SPARK_HOME /opt/spark
COPY --from=build ${SPARK_HOME}/jars ${SPARK_HOME}/jars
COPY --from=build ${SPARK_HOME}/RELEASE ${SPARK_HOME}/RELEASE
COPY --from=build ${SPARK_HOME}/bin ${SPARK_HOME}/bin
COPY --from=build ${SPARK_HOME}/sbin ${SPARK_HOME}/sbin
COPY --from=build ${SPARK_HOME}/kubernetes/dockerfiles/spark/entrypoint.sh /opt/
COPY --from=build ${SPARK_HOME}/kubernetes/dockerfiles/spark/decom.sh /opt/
COPY --from=build ${SPARK_HOME}/data ${SPARK_HOME}/data

WORKDIR ${SPARK_HOME}/work-dir
RUN chmod g+w ${SPARK_HOME}/work-dir
RUN chmod a+x /opt/decom.sh

# Install python packages (other than pyspark) using pip for pyspark to work.
# Pyspark is installed in the COPY instruction below
RUN mkdir ${SPARK_HOME}/python && \
    pip3 install --upgrade pip setuptools && \
    pip3 install --upgrade --trusted-host pypi.org --trusted-host files.pythonhosted.org pyyaml pyarrow boto3 && \
    rm -rf /root/.cache && rm -rf /var/cache/apt/* && rm -rf /var/lib/apt/lists/*

COPY --from=build ${SPARK_HOME}/python/pyspark ${SPARK_HOME}/python/pyspark
COPY --from=build ${SPARK_HOME}/python/lib ${SPARK_HOME}/python/lib

# set Spark user
ARG spark_uid=185
RUN echo "spark:x:${spark_uid}:0::/opt/spark/work-dir:/bin/bash" >> /etc/passwd
WORKDIR ${SPARK_HOME}/work-dir

# Specify the User that the actual main process will run as
USER ${spark_uid}

# add pyspark, py4j to PYTHONPATH
ENV PYTHONPATH=${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}

ENTRYPOINT [ "/opt/entrypoint.sh" ]