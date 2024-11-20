from pyspark.sql import SparkSession

def mysql_to_s3():
    spark = SparkSession.builder \
        .appName("MySQL_to_S3") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

    jdbc_url = "jdbc:mysql://mysql:3306/test_db"
    table = "dummy_data"

    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", "root") \
        .option("password", "password") \
        .load()

    # Writing to S3
    df.write.mode("overwrite").parquet("s3a://my-bucket/dummy-data/")

if __name__ == "__main__":
    mysql_to_s3()
