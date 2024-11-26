import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pandas as pd


def generate_random_string(length=10):
    """Generates a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def create_spark_session():
    """Creates a Spark session with the necessary JDBC driver for MySQL."""
    spark = SparkSession.builder \
        .appName("Insert Random Records into MySQL") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .getOrCreate()
    return spark


def insert_random_records():
    """Inserts 10 random records into the MySQL database using Spark."""
    # Generate random data
    data = [(generate_random_string(), random.randint(1, 100)) for _ in range(10)]

    # Convert the data to a DataFrame
    df = pd.DataFrame(data, columns=["name", "value"])

    # Create a Spark DataFrame from the pandas DataFrame
    spark_df = create_spark_session().createDataFrame(df)

    # MySQL connection properties
    jdbc_url = "jdbc:mysql://mysql:3306/test_db"  # MySQL service name and database name from docker-compose
    connection_properties = {
        "user": "airflow",
        "password": "airflow_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Write the DataFrame to MySQL using Spark JDBC
    spark_df.write.jdbc(url=jdbc_url, table="random_data", mode="append", properties=connection_properties)


# Call the function to insert the records
insert_random_records()
