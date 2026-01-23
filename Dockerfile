FROM apache/airflow:3.1.6

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

ENV SPARK_JARS_DIR=/opt/airflow/jars
ENV POSTGRES_JDBC_JAR=postgresql-42.7.8.jar
RUN mkdir -p ${SPARK_JARS_DIR} \
    && curl -L https://jdbc.postgresql.org/download/${POSTGRES_JDBC_JAR} \
    -o ${SPARK_JARS_DIR}/${POSTGRES_JDBC_JAR}

USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt