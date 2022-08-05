import airflow.utils.dates
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from my_modules.GitHubToS3BucketOperator import GitHubToS3Operator

default_args = {
    "owner": "mafer",
     "start_date": airflow.utils.dates.days_ago(1)
}
with DAG(
    "dag_insert_data_to_s3", default_args=default_args, schedule_interval="@once"
) as dag:
        insert_data_bucket = GitHubToS3Operator(
        task_id="dag_github_to_s3",
    )
(
    insert_data_bucket
)