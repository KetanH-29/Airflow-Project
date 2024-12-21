from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, max as spark_max
from datetime import datetime
import yaml
import os

def extract_and_write_to_s3():
    """Extract records with dates greater than the max date in S3 mirror paths and save them to S3 in Parquet format."""

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL to S3 Incremental") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Set log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    # Load secrets from the YAML file
    app_secrets_path = "/opt/bitnami/spark/.secrets"
    with open(app_secrets_path) as secret_file:
        app_secret = yaml.load(secret_file, Loader=yaml.FullLoader)

    # Set Hadoop configuration for AWS S3 credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # MySQL JDBC connection details
    jdbc_url = "jdbc:mysql://mysql:3306/test_db"
    jdbc_properties = {
        "user": "airflow",
        "password": "airflow_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Define S3 mirror paths
    base_path = "s3a://ketan-mirror-bucket/MySQL_DB/test_db/tables/previous/current/full_load/"
    current_date = datetime.now()
    year, month, day = current_date.year, f"{current_date.month:02d}", f"{current_date.day:02d}"
    source_path_staging0 = f"{base_path}transactions/year={year}/month={month}/day={day}/"
    source_path_staging1 = f"{base_path}customers/year={year}/month={month}/day={day}/"
    source_path_staging2 = f"{base_path}campaigns/year={year}/month={month}/day={day}/"

    # Define S3 staging paths for output
    output_path0 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/upsert/tables/transactions/"
    output_path1 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/upsert/tables/customers/"
    output_path2 = "s3a://ketan-staging-bucket/MySQL_DB/test_db/upsert/tables/campaigns/"

    # Function to get max date from S3
    def get_max_date(s3_path):
        try:
            s3_df = spark.read.parquet(s3_path)
            max_date = s3_df.agg(spark_max("date").alias("max_date")).collect()[0]["max_date"]
            return max_date
        except Exception as e:
            print(f"Could not fetch max date from {s3_path}: {e}")
            return None

    # Get max dates from S3 paths
    max_date_transactions = get_max_date(source_path_staging0)
    max_date_customers = get_max_date(source_path_staging1)
    max_date_campaigns = get_max_date(source_path_staging2)

    print(f"Max Date - Transactions: {max_date_transactions}")
    print(f"Max Date - Customers: {max_date_customers}")
    print(f"Max Date - Campaigns: {max_date_campaigns}")

    # Read data from MySQL with incremental filtering
    df0 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "transactions") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    if max_date_transactions:
        df0 = df0.filter(col("date") > lit(max_date_transactions))

    df1 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "customers") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    if max_date_customers:
        df1 = df1.filter(col("date") > lit(max_date_customers))

    df2 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "campaigns") \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    if max_date_campaigns:
        df2 = df2.filter(col("date") > lit(max_date_campaigns))

    # Add year, month, and day columns for partitioning
    df0 = df0.withColumn("year", lit(year)) \
             .withColumn("month", lit(month)) \
             .withColumn("day", lit(day))

    df1 = df1.withColumn("year", lit(year)) \
             .withColumn("month", lit(month)) \
             .withColumn("day", lit(day))

    df2 = df2.withColumn("year", lit(year)) \
             .withColumn("month", lit(month)) \
             .withColumn("day", lit(day))

    # Write data to S3 in Parquet format with partitioning
    df0.write.partitionBy("year", "month", "day").parquet(output_path0, mode="overwrite")
    df0.show()
    print(f"Data written to {output_path0}")

    df1.write.partitionBy("year", "month", "day").parquet(output_path1, mode="overwrite")
    df1.show()
    print(f"Data written to {output_path1}")

    df2.write.partitionBy("year", "month", "day").parquet(output_path2, mode="overwrite")
    df2.show()
    print(f"Data written to {output_path2}")

if __name__ == "__main__":
    extract_and_write_to_s3()


# spark-submit --jars /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jobs/data_extractor_writer_incremental_upsert.py

