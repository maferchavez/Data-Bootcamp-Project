#Example of an ingestion of data on a PostgreSQL

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator #an operator to fill with something, it does not do anything
from airflow.utils.dates import days_ago #used it for the start_date of dag inizialization
#from sqlalchemy import create_engine 
from airflow.providers.postgres.operators.postgres import PostgresOperator # used for prepare task to setup environment for landing data
from airflow.operators.python import PythonOperator #this module is for the load step
from airflow.providers.postgres.hooks.postgres import PostgresHook #this module is for make the connection between postgres and airflow
import pandas as pd #used for extract data from zip
import sqlite3 #used in method of convert dataframe to sql and load to table created

#function for python operator in load step 
def ingest_data():
    hook = PostgresHook(postgres_conn_id='example') #create a hook and connection
    #get_postgres_conn = hook.get_conn()
    #curr = get_postgres_conn.cursor('cursor')
    conn = sqlite3.connect('user_purchase')
    c= conn.cursor()
    df = pd.read_csv("https://raw.githubusercontent.com/maferchavez/Data-Bootcamp-Project/main/raw_data/user_purchase.zip")
    df.to_sql('user_purchase', conn, if_exists='replace', index = False)

def verify_data_func():
    sql_stmt = "SELECT COUNT(*) FROM user_purchase"
    hook = PostgresHook(postgres_conn_id='example')
    pg_conn = hook.get_conn()
    cursor = pg_conn.cursor()
    count = cursor.excecute(sql_stmt)
    print (count)
    
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
        postgres_conn_id = 'example',#how to do a connection via airflow?
        sql = """
                CREATE TABLE IF NOT EXISTS user_purchase(
                    invoice_number varchar(10),
                    stock_code varchar(20),
                    detail varchar(1000),
                    quantity int,
                    invoice_date timestamp,
                    unit_price numeric(8,3),
                    customer_id int,
                    country varchar(20)
                            );
                            """) 
    load = PythonOperator(task_id='load', python_callable=ingest_data)
    verify_data = PythonOperator(task_id='verify_data', python_callable=verify_data_func)
    end_workflow = DummyOperator(task_id='end_workflow')

    #set up workflow
    start_workflow >> validate >> prepare >> load >> verify_data >> end_workflow