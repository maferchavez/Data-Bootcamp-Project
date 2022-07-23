#Example of an ingestion of data on a PostgreSQL

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator #an operator to fill with something, it does not do anything
from airflow.utils.dates import days_ago #used it for the start_date of dag inizialization
from airflow.providers.postgres.operator.postgres import PostgresOperator # used for prepare task to setup environment for landing data

with DAG("db_ingestion", start_date = days_ago(1), schedule_interval = '@once') as dag:#Instanciate and load a DAG object
    """
    Defing tasks of a workflow process
        VALIDATE: Validate the presence of source data.
        PREPARE: Set up the database for all the necesary components.
        LOAD: Load the source data to a database.
    """
    start_workflow = DummyOperator(task_id='start_workflow') 
    validate = DummyOperator(task_id='validate') 
    prepare = PostgresOperator(
        task_id='prepare',
        postgres_conn_id = 'ml_conn',#how to do a connection via airflow?
        sql = '') 
    load = DummyOperator(task_id='load') 
    end_workflow = DummyOperator(task_id='end_workflow')

    #set up workflow
    start_workflow >> validate >> prepare >> load >> end_workflow