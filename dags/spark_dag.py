from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)


#Configurations for upload data to S3 bucket
BUCKET_NAME = 'Mafer_bucket_DEB'
local_data = "C:/Users/maryf/Documents/Data-bootcamp/Data-Bootcamp-Project/raw_data/movie_review.zip"
s3_data = 'raw_data/movie_review.zip'
local_script = 'C:/Users/maryf/Documents/Data-bootcamp/Data-Bootcamp-Project/dags/spark_scripts/Processing_movie_review.py'
s3_script = 'spark_scripts/Processing_movie_review.py'

# Configurations for create an EMR cluster
JOB_FLOW_OVERRIDES = {
    'Name': 'Processing data', # This is the name of the EMR cluster
    'RealeaseLabel': 'emr-5.29.0', #EMR version
    'Applications': [{'Name':'Hadoop'}, {'Name': 'Spark'}], # This includes the "features" of the culster
    'Configurations': [ # By default EMR uses Python2, it will be change to Python3
        {
            'Classification': 'spark-env', # Venv for "installing" Python3
            'Configurations':[
                {
                    'Classification': 'export', 
                    'Properties':{'PYSPARK_PYTHON': 'usr/bin/python3'}
                }
            ]
        }
    ],
    'Instance   s': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1
            },
            {
                'Name': 'Core-2',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': 1
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole'
}

# Configuration of EMR steps
s3_clean = 'clean/data' #create the path to put the data once processed
SPARK_STEPS = [
    {
        'Name': 'Move raw data from S3 to HDFS', # Name of the step, HDFS means Hadoop Distributed File System
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep':{
            'Jar': 'command-runner.jar',
            'Args': [
                's3-dist-cp', # Using S3 Distributed Copy EMR tool that copy data from S3 to EMR clusters HDFS location
                '--src=s3://{{ params.BUCKET_NAME }}/data', # Source of data
                '--dest=/movie' # Destination of data
            ]
        }
    },
    {
        'Name': 'Classify movie reviews',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args':[
                'spark-submit', # This is for submmiting a spark job using the spark script in S3
                '--deploy-mode',
                'client',
                's3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}'
            ]
        }
    },
    {
        'Name': 'Move clean data from HDFS to S3',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args':[
                's3-dist-cp',
                '--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}'
            ]   
        }
    }
]

def upload_to_s3 (filename, key, bucket_name = BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename = filename, bucket_name = bucket_name, replace = True, key = key)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstram': True,
    'start_date': datetime(2022, 8, 5),
    'email': [airflow@airflow.com],
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spark_submit_airflow',
    default_args = default_args,
    schedule_interval = '0 10 * * *',
    max_active_runs = 1
)
# 0. start pipeline
start_pipeline_emr = DummyOperator(task_id = 'start_pipeline_emr')

# 1. Move data and scripts to the cloud (S3)
# DAG for data
data_to_s3 = PythonOperator( 
    dag = dag, 
    task_id = 'data_to_S3',
    python_callable = upload_to_s3,
    op_kwargs = {'filename': local_data, 'key':s3_data}
)

# DAG for scripts
script_to_s3 = PythonOperator(
    dag = dag, 
    task_id = 'script_to_S3', 
    python_callable = upload_to_s3, 
    op_kwargs = {'filename': local_script, 'key': s3_script}
)

# 2. Create an EMR Cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id = 'create_emr_cluster',
    job_fow_overrrides = JOB_FLOW_OVERRIDES,
    aws_conn_id = 'aws_default', 
    emr_conn_id = 'emr_default', 
    dag = dag
)

# 3. Add steps to EMR cluster
add_step = EmrAddStepsOperator(
    task_id = 'Add steps',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster',key='return_value) }}",
    aws_conn_id = 'aws_default',
    steps = SPARK_STEPS,
    params = {  #Parameters to fill in the SPARK_STEPS json
        'BUCKET_NAME': BUCKET_NAME,
        's3_data': s3_data,
        's3_script': s3_script,
        's3_clean': s3_clean
    },
    dag = dag
)

# 4. Test that we have done all the steps
last_step =len(SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    +"] }}",
    aws_conn_id = 'aws_defaul',
    dag=dag
    )
 
 # 5. Terminate EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id = 'terminate_emr_cluster',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id = 'aws_default',
    dag = dag
)

# 6. End pipeline
end_pipeline = DummyOperator(task_id = 'end_pipeline', dag = dag)

# Order tasks
start_pipeline_emr >> [data_to_s3, script_to_s3] >> create_emr_cluster
create_emr_cluster >> add_step >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_pipeline