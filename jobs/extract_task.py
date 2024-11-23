import pymysql
import pandas as pd

def extract_mysql_data():
    connection = pymysql.connect(
        host='mysql',  # Ensure this matches your Docker service name for MySQL
        user='airflow',
        password='airflow_password',
        database='test_db'
    )
    query = "SELECT * FROM random_data"
    df = pd.read_sql(query, connection)
    # Ensure the directory exists in your container
    output_path = '/opt/airflow/local_data/csv_data/temp_data.csv'
    df.to_csv(output_path, index=False)
    print(f"Data extracted to {output_path}")
    connection.close()
