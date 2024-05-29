# use the latest open JRE 17 minimal as the build image
ARG OPENJRE_TAG=17.0.11_9-jre-ubi9-minimal
FROM eclipse-temurin:${OPENJRE_TAG} as build

# download requested Spark distribution and extract into /opt/spark
ARG SPARK_VERSION=3.5.1
ARG HADOOP_MAJOR_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION} /opt/spark

# download the jars needed for spark to work with s3
WORKDIR /opt/spark/jars
# this must be the Hadoop version that is used in Spark 3.5.1
ARG HADOOP_VERSION=3.3.4
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar

# These AWS jars are needed by Hadoop to work with S3
# There is a new release for these jars each day, any version >= 1.12.305 should work
ARG AWS_JAVA_VERSION=1.12.724
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/${AWS_JAVA_VERSION}/aws-java-sdk-${AWS_JAVA_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/${AWS_JAVA_VERSION}/aws-java-sdk-s3-${AWS_JAVA_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/${AWS_JAVA_VERSION}/aws-java-sdk-core-${AWS_JAVA_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/${AWS_JAVA_VERSION}/aws-java-sdk-dynamodb-${AWS_JAVA_VERSION}.jar

# Add further jar files needed to run S3 in Kubernetes
RUN wget https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.6.2/woodstox-core-6.6.2.jar && \
    wget https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.2/stax2-api-4.2.2.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.10.1/commons-configuration2-2.10.1.jar && \
    wget https://repo1.maven.org/maven2/com/google/re2j/re2j/1.7/re2j-1.7.jar

RUN rm hadoop-shaded-guava-1.1.1.jar

# use the latest python 3.11 slim bookworm image
# ARG PYTHON_IMAGE=python:3.11.9-slim-bookworm
# FROM $PYTHON_IMAGE
FROM python:3.11.9-slim-bookworm

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
RUN echo "spark:x:185:0::/opt/spark/work-dir:/bin/bash" >> /etc/passwd
WORKDIR ${SPARK_HOME}/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}

# add pyspark, py4j to PYTHONPATH
ENV PYTHONPATH=${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}
