from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG
with DAG(
    "mysql_to_parquet_dag",
    description="Read data from MySQL and write to Parquet using Spark",
    schedule_interval="@daily",  # Adjust the schedule as needed
    start_date=datetime(2024, 11, 21),
    catchup=False,
) as dag:

    # Define SparkSubmitOperator to run the Spark job
    spark_submit_task = SparkSubmitOperator(
        task_id="mysql_to_parquet",
        conn_id="spark_default",  # Spark connection in Airflow
        application="/opt/airflow/jobs/mysql_to_parquet.py",  # Path to the Spark Python script in the Airflow container
        jars="/opt/airflow/jobs/mysql-connector-java-8.0.28.jar",  # Path to the MySQL JDBC driver .jar file
        name="mysql_to_parquet_job",
        application_args=[
            "jdbc:mysql://mysql:3306/test_db",  # MySQL connection URL
            "airflow",  # MySQL username
            "airflow_password",  # MySQL password
            "random_data",  # Table name
            "local_data/random_data.parquet"  # Output Parquet file path
        ],
    )

    spark_submit_task
