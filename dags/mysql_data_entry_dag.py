import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Add the jobs directory to Python's path
sys.path.append('/opt/airflow/jobs')

# Import the function from data_creator
from data_creator import insert_random_records

# Define the DAG
with DAG(
    dag_id="insert_random_records_dag",
    description="Insert 10 random records into MySQL using a Python script",
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 21),
    catchup=False,
) as dag:

    # Define the PythonOperator to execute the data insertion task
    insert_records_task = PythonOperator(
        task_id="insert_random_records_task",
        python_callable=insert_random_records
    )

    # Set task dependencies
    insert_records_task