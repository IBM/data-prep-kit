# see https://github.com/apache/spark-docker/blob/master/3.5.1/scala2.12-java17-ubuntu/Dockerfile
# and https://github.com/apache/spark-docker/blob/master/3.5.1/scala2.12-java17-python3-ubuntu/Dockerfile
# The reasons we are modifying Apache dockerfile are:
#   1. We support cofiguration of the Python version
#   2. We are setting non root user

FROM eclipse-temurin:17-jre-jammy

# versions
ARG SPARK_VERSION=3.5.1
ARG HADOOP_MAJOR_VERSION=3
ARG PYTHON_VERSION=3.10
ARG spark_uid=1001

# set Spark user
RUN groupadd --system --gid=${spark_uid} spark && \
    useradd --system --uid=${spark_uid} --gid=spark spark

# Install python and libraries required by Spark
RUN set -ex; \
    apt-get update; \
    apt-get install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu libnss-wrapper; \
    apt-get install -y python${PYTHON_VERSION} python3-pip; \
    mkdir -p /opt/spark; \
    mkdir /opt/spark/python; \
    mkdir -p /opt/spark/examples; \
    mkdir -p /opt/spark/work-dir; \
    chmod g+w /opt/spark/work-dir; \
    touch /opt/spark/RELEASE; \
    chown -R spark:spark /opt/spark; \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su; \
    rm -rf /var/lib/apt/lists/*

# download requested Spark distribution and extract into /opt/spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz

# copy Spark components
RUN mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/jars /opt/spark/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/bin /opt/spark/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/sbin /opt/spark/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/kubernetes/dockerfiles/spark/decom.sh /opt/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/kubernetes/dockerfiles/spark/entrypoint.sh /opt/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/examples /opt/spark/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/kubernetes/tests /opt/spark/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/python/pyspark /opt/spark/python/pyspark/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/python/lib /opt/spark/python/lib/; \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}/data /opt/spark/; \
    chmod a+x /opt/decom.sh \

# cleanup
RUN rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz; \
    rm -R spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}

# upgrade Python setup tools
RUN pip3 install --upgrade pip setuptools

# Set environment \
ENV SPARK_HOME /opt/spark
WORKDIR /opt/spark/work-dir

# add pyspark, py4j to PYTHONPATH
ENV PYTHONPATH=${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}

# entry point
ENTRYPOINT [ "/opt/entrypoint.sh" ]