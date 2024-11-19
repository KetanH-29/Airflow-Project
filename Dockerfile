FROM apache/airflow:2.7.1-python3.11

USER root

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev default-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Install necessary Python packages with constraints
ARG AIRFLOW_VERSION=2.7.1
ARG AIRFLOW_CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

RUN pip install \
    apache-airflow-providers-apache-spark \
    pyspark \
    --constraint "${AIRFLOW_CONSTRAINT_URL}"
