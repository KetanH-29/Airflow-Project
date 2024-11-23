from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from jobs.extract_task import extract_mysql_data  # Import extract function
from jobs.transform_write_task import write_to_parquet  # Import transform function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 23),
}

dag = DAG(
    'mysql_to_parquet',
    default_args=default_args,
    description='Extract MySQL data and write to Parquet',
    schedule_interval=None,
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_mysql_data',
    python_callable=extract_mysql_data,  # Reference the imported function
    dag=dag,
)

transform_task = PythonOperator(
    task_id='write_to_parquet',
    python_callable=write_to_parquet,  # Reference the imported function
    dag=dag,
)

# Task dependencies
extract_task >> transform_task
