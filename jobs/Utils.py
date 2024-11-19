import os
import sys
from pyspark.sql import SparkSession
from datetime import datetime

# Create a Spark session
def create_spark_session(tablename):
    return SparkSession.builder \
        .appName(tablename) \
        .getOrCreate()

# Read data from MySQL
def read_data_from_mysql(tablename):
    mysql_url = "jdbc:mysql://localhost:3306/my_db"
    table_name = tablename
    user = "root"
    password = "your_password"  # Make sure to provide the correct password

    # Load data from MySQL using JDBC
    df = spark.read.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()
    return df

# Write DataFrame to Parquet
def write_df(df):
    # Get the current date to dynamically update the output path
    current_date = datetime.now().strftime("%Y=%Y/%m=%m/%d=%d")
    output_path = f"/staging/sourcename/dbname/tablename/{current_date}"

    df.write.mode('overwrite').parquet(output_path)
    print(f"Data written to: {output_path}")

# Main function
def main(table_name, load_type):
    spark = create_spark_session(table_name)

    if load_type == 'full':
        in_df = read_data_from_mysql(table_name)
        write_df(in_df)

# Entry point for the script
if __name__ == "__main__":
    table_name = sys.argv[1]  # Table name passed as the first argument
    load_type = sys.argv[2]   # Load type (full) passed as the second argument
    main(table_name, load_type)
