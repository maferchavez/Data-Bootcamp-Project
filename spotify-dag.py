from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'ariflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['maryfer.chavezromo@live.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retray_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description= 'Spotify-dag for running etl',
    schedule_interval='@daily'
)


task1 = PythonOperator(
    task_id = 'spotify_etl',
    python_callable =  run_spotify_etl,
    dag = dag
)

task1