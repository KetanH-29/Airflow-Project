FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system dependencies
USER root

# Install necessary system dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev default-jdk wget && \
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

# Copy the MySQL connector JAR file into the container for both Spark and Airflow
COPY ./jars/mysql-connector-java-8.0.30.jar /opt/airflow/jars/mysql-connector-java-8.0.30.jar

# Verify installation of pymysql and that the connector JAR is in place
RUN python -m pip show pymysql && \
    ls /opt/airflow/jars/mysql-connector-java-8.0.30.jar
