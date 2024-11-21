import sys
from pyspark.sql import SparkSession

# Read command line arguments
jdbc_url = sys.argv[1]  # MySQL connection URL
user = sys.argv[2]  # MySQL username
password = sys.argv[3]  # MySQL password
table_name = sys.argv[4]  # MySQL table name
output_path = sys.argv[5]  # Parquet file output path

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL to Parquet") \
    .config("spark.jars", "/opt/airflow/jobs/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

# MySQL connection properties
properties = {
    "user": user,
    "password": password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Write data to a Parquet file locally
df.write.parquet(output_path, mode="overwrite")

# Stop the Spark session
spark.stop()
