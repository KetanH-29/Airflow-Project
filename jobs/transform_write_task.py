from pyspark.sql import SparkSession
import os

def write_to_parquet():
    """Reads a CSV file and writes it to Parquet format."""
    spark = SparkSession.builder \
        .appName("MySQL to Parquet") \
        .getOrCreate()

    input_path = '/opt/airflow/local_data/csv_data/temp_data.csv'
    output_path = '/opt/airflow/local_data/parquet_data/output_data.parquet'

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found at {input_path}")

    # Read CSV and write to Parquet
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.parquet(output_path, mode='overwrite')
    print(f"Data written to {output_path}")

if __name__ == "__main__":
    write_to_parquet()
