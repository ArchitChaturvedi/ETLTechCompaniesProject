from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from scripts.extract import extract_csv
from scripts.transform import transform_data
from scripts.load import load_to_postgres

default_args = {
    'owner': 'archit',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='csv_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL pipeline for 4 CSV files',
) as dag:

    csv_files = [
        'consumer_electronics_companies_2024.csv',
        'cybersecurity_companies_2024.csv',
        'semiconductor_companies_2024.csv',
        'tech_companies_2024.csv'
    ]

    for file in csv_files:
        file_name = os.path.splitext(file)[0]

        extract_task = PythonOperator(
            task_id=f'extract_{file_name}',
            python_callable=extract_csv,
            op_kwargs={'filename': file},
        )

        transform_task = PythonOperator(
            task_id=f'transform_{file_name}',
            python_callable=transform_data,
            op_kwargs={'filename': file},
        )

        load_task = PythonOperator(
            task_id=f'load_{file_name}',
            python_callable=load_to_postgres,
            op_kwargs={'filename': file},
        )

        extract_task >> transform_task >> load_task
