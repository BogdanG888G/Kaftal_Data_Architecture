FROM apache/airflow:3.1.6

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/spark/jars && \
    cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar && \
    chown -R airflow:root /opt/spark/jars

ENV SPARK_JARS_DIR=/opt/spark/jars

USER airflow

RUN mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    pyspark==3.5.3 \
    boto3 \
    pyarrow \
    dbt-core \
    dbt-trino