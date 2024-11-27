from pyspark.sql import SparkSession

def extract_mysql_data():
    """Extract data from MySQL and save it to CSV."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL to Spark") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .getOrCreate()

    # MySQL JDBC connection details
    jdbc_url = "jdbc:mysql://mysql:3306/test_db"
    jdbc_properties = {
        "user": "airflow",
        "password": "airflow_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Read data from MySQL
    df = spark.read.jdbc(url=jdbc_url, table="random_data", properties=jdbc_properties)

    # Save to CSV
    output_path = "/opt/airflow/local_data/csv_data/temp_data.csv"
    df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Data extracted to {output_path}")

if __name__ == "__main__":
    extract_mysql_data()
