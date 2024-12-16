from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import yaml
import os.path

jdbc_url = "jdbc:mysql://mysql:3306/test_db"
jdbc_properties = {
    "user": "airflow",  # Replace with your MySQL username if necessary
    "password": "airflow_password",  # Replace with your MySQL password if necessary
    "driver": "com.mysql.cj.jdbc.Driver"
}


def get_incremental_df(maxvalue,spark,tablename, incr_col):
    query = f""" SELECT * FROM {table}
                    WHERE {incr_col} > '{maxvalue}'"""
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{tablename}") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()



def extract_and_write_to_s3(tablename, loadtype, primarykey,incr_col):
    """Extract data from MySQL and save it to S3 in Parquet format."""

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL to S3") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Set log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_secrets_path = "/opt/bitnami/spark/.secrets"

    # Load secrets from the YAML file
    with open(app_secrets_path) as secret_file:
        app_secret = yaml.load(secret_file, Loader=yaml.FullLoader)

    # Set Hadoop configuration for AWS S3 credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])


    if loadtype == 'full':
        # Read data from MySQL
        df0 = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{tablename}") \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .load()

        print("Schema of the data read from MySQL:")
        df0.printSchema()
        df0.show()

        # Get the current date
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        current_day = current_date.day

        # Add year, month, and day columns for partitioning
        df0 = df0.withColumn("year", lit(current_year)) \
               .withColumn("month", lit(f"{current_month:02d}")) \
               .withColumn("day", lit(f"{current_day:02d}"))

        # Define S3 output path
        output_path0 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/tables/{tablename}/"

        # Write data to S3 in Parquet format with partitioning
        df0.write.partitionBy("year", "month", "day").parquet(output_path0, mode="overwrite")
        print(f"Data extracted from MySQL and written to {output_path0} on S3")
    elif loadtype == 'append':
        # Query to fetch the record with the maximum date
        query0 = """
            SELECT * FROM transactions
            WHERE date = (SELECT MAX(date) FROM transactions)
            """

        # Read data from MySQL
        df0 = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({query0}) AS max_date_record_transactions") \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .load()

        # Print schema of the data for verification
        print("Schema0 of the data read from transactions table MySQL:")
        df0.printSchema()
        df0.show()
        mirror_path = "s3a://ketan-mirror-bucket/MySQL_DB/test_db/append/tables/transactions/"
        # Get the current date
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        current_day = current_date.day
        try:
            print(f"Attempting to read data from {source_path_staging0}...")
            df_staging0 = spark.read.parquet(mirror_path)
            print("Data found. Copying to destination...")
            #logic for getting max value----- to be added
            df = get_incremental_df(maxvalue,spark,tablename,incr_col)
            #logic to write to staging and then read from staging and append to mirror current
        except AnalysisException:
            print(f"Data is absent at {source_path_staging0}. Skipping process.")
            return

        # Add year, month, and day columns for partitioning
        df0 = df0.withColumn("year", lit(current_year)) \
            .withColumn("month", lit(f"{current_month:02d}")) \
            .withColumn("day", lit(f"{current_day:02d}"))



        # Write data to S3 in Parquet format with partitioning
        df0.write.partitionBy("year", "month", "day").parquet(output_path0, mode="overwrite")
        print(f"Data extracted from MySQL (max date record) and written to {output_path0} on S3")


if __name__ == "__main__":
    extract_and_write_to_s3()

#spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/data_extractor_writer_full_load.py
