from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from my_modules.GithubToS3BucketOperator import GithubToS3BucketOperator

default_args = {'owner': 'airflow',
                'start_date': datetime(2018, 1, 5),
                # 'email': ['YOUREMAILHERE.com'],
                'email_on_failure': True,
                'email_on_retry': False
                }

dag = DAG('github_to_redshift',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False
          )

# Connection creds.
aws_conn_id = 'astronomer-redsift-dev'
s3_conn_id = 'astronomer-s3'
s3_bucket = 'astronomer-workflows-dev'


# Copy command params.
copy_params = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

# Github Endpoints
endpoints = [{"name": "issues",
              "payload": {"state": "all"},
              "load_type": "rebuild"},
             {"name": "members",
              "payload": {},
              "load_type": "rebuild"}]

# Github Orgs (cut a few out for faster)
orgs = [{'name': 'maferchavez',
         'github_conn_id': 'maferchavez-github'},
        {'name': 'airflow-plugins',
         'github_conn_id': 'maferchavez-github'}]

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    finished_api_calls = DummyOperator(task_id='finished_api_calls')

    for endpoint in endpoints:
        for org in orgs:
            github = GithubToS3Operator(task_id='github_{0}_data_from_{1}_to_s3'.format(endpoint['name'], org['name']),
                                        github_conn_id=org['github_conn_id'],
                                        github_org=org['name'],
                                        github_repo='all',
                                        github_object=endpoint['name'],
                                        payload=endpoint['payload'],
                                        s3_conn_id=s3_conn_id,
                                        s3_bucket=s3_bucket,
                                        s3_key='github/{0}/{1}.json'
                                        .format(org['name'], endpoint['name']))

kick_off_dag >> github >> finished_api_calls
    