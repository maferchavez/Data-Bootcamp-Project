import airflow
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

with airflow.DAG('sparkDAG',
                    start_date = days_ago(1), 
                    schedule_interval = '@once'
                ) as dag:
    movie_review_task = BashOperator(
        task_id='processing_movie_reviews',
        bash_command="python ./dags/spark_scripts/Processing_movie_review.py",
    )