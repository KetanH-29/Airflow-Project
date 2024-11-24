#FROM apache/airflow:2.7.1-python3.11
#
#USER root
#
## Install necessary system dependencies
#RUN apt-get update && \
#    apt-get install -y gcc python3-dev default-jdk && \
#    apt-get clean
#
## Set JAVA_HOME environment variable
#ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
#
#USER airflow
#
## Install necessary Python packages with constraints
#ARG AIRFLOW_VERSION=2.7.1
#ARG AIRFLOW_CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
#
#RUN pip install \
#    apache-airflow-providers-apache-spark \
#    pyspark \
#    pandas \
#    --constraint "${AIRFLOW_CONSTRAINT_URL}"
#
#RUN pip install pymysql


FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system dependencies
USER root

# Install necessary system dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev default-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Switch back to the airflow user
USER airflow

# Install necessary Python packages with Airflow constraints
ARG AIRFLOW_VERSION=2.7.1
ARG AIRFLOW_CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# Install Airflow and additional dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark \
    pandas \
    pymysql \
    --constraint "${AIRFLOW_CONSTRAINT_URL}"

# Verify installation of pymysql
RUN python -m pip show pymysql
