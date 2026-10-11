FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    colorama\
    pandas \
    openpyxl\
    scikit-learn\
    kafka-python==2.0.2 

ENV PROJECT_ROOT=/opt/airflow/retail_pipeline    