#CSV to S3
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)






with DAG(
    dag_id='s3_dag',
    schedule_interval='@once',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'https://raw.githubusercontent.com/maferchavez/Data-Bootcamp-Project/main/data/log_reviews.csv',
            'key': 'log_reviews.csv',
            'bucket_name': 's3-data-bootcamp'
        }
    )