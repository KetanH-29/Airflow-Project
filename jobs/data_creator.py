import random
import string
from pyspark.sql import SparkSession
import pandas as pd


def generate_random_string(length=10):
    """Generates a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_spark_session():
    """Creates a Spark session with the necessary JDBC driver for MySQL."""
    spark = SparkSession.builder \
        .appName("Insert Random Records into MySQL") \
        .master("local") \
        .config("spark.jars", "file:///C:/Spark/jars/mysql-connector-java-8.0.30.jar") \
        .getOrCreate()
    return spark

def insert_random_records():
    """Inserts 10 random records into the MySQL database using Spark."""
    # Generate random data
    data = [(generate_random_string(), random.randint(1, 100)) for _ in range(10)]

    # Convert the data to a DataFrame
    df = pd.DataFrame(data, columns=["name", "value"])

    # Create a Spark DataFrame from the pandas DataFrame
    spark = create_spark_session()
    spark_df = spark.createDataFrame(df)

    # MySQL connection properties
    jdbc_url = "jdbc:mysql://localhost:3306/test_db"  # MySQL service name and database name
    connection_properties = {
        "user": "airflow",
        "password": "airflow_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Write the DataFrame to MySQL using Spark JDBC
    spark_df.write.jdbc(url=jdbc_url, table="random_data", mode="append", properties=connection_properties)

if __name__ == "__main__":
    insert_random_records()
