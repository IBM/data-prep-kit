ARG BASE_IMAGE=pyspark-base:3.5.1
# local kind test
#ARG BASE_IMAGE=spark-fdedup/pyspark-base-1:latest
FROM ${BASE_IMAGE}

USER root
RUN mkdir -p /opt/spark/work-dir/src/templates && \
    mkdir -p /opt/spark/work-dir/config && \
    mkdir -p /opt/spark/work-dir/src/transforms

# install requirements from requirements.txt
COPY requirements_fuzzy_dedup.txt .
RUN pip3 install -r requirements_fuzzy_dedup.txt

COPY src/spark_transformer_runtime.py /opt/spark/work-dir/src/
COPY src/data_access.py /opt/spark/work-dir/src/
COPY src/Murmur_MH.py /opt/spark/work-dir/src/
COPY src/config.yml /opt/spark/work-dir/src/
COPY src/fd_signature_calculator.py /opt/spark/work-dir/src/
COPY src/fd_clusters_calculator.py /opt/spark/work-dir/src/
COPY src/fd_jaccard_distance_calculator.py /opt/spark/work-dir/src/
COPY src/fd_clean_data.py /opt/spark/work-dir/src/
COPY src/spark_fuzzy_controller.py /opt/spark/work-dir/src/
COPY deployment/kubernetes/spark-executor-pod-template.yaml /opt/spark/work-dir/src/templates/
COPY config/spark_profile.yaml /opt/spark/work-dir/config/

USER spark
# add src folder to PYTHONPATH
ENV PYTHONPATH=${SPARK_HOME}/work-dir/src/:${PYTHONPATH}
