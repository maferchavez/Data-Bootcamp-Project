import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta, datetime
from psycopg2.extras import execute_values

#DAG default arguments

default_args = {
    'owner': 'Mafer Chávez',
    'depends_on_past': False,
    'email': ['maryfer.chavezromo@gmail.com'],
    'email_on_failure': True,
    'retries' : 1,
    'retries_delay': timedelta(minutes=1)
}

#DAG assigment
dag = DAG('insert_data_postgres', default_args = default_args, schedule_interval='@once', catchup=False)

def csv_to_postgres():
    #open PostgresSQL connection
    pg_hook = PostgresHook(postgres_conn_id='conn_postgress')
    get_postgres_conn = pg_hook.get_connn()
    curr = get_postgres_conn.cursor('cursor')

    #CSV loading to table
    file = 'Data-Bootcamp-Project/data/user_purchase.csv'
    with open(file, 'r') as f:
        next (f)
        curr.copy_from(f, 'user_purchase',sep=',')
        get_postgres_conn.commit() 

task1 = PostgresOperator(task_id= 'crate_table', 
                            sql="""
                            CREATE A TABLEIF NOT EXISTS user_purchase(
                                invoice_number varchar(10),
                                stock_code varchar(20),
                                detail varchar(1000),
                                quantity int,
                                invoice_date timestamp,
                                unit_price numeric(8,3),
                                customer_id int,
                                country varchar(20)
                            );
                            """,
                            postgres_conn_id = 'conn_postgres',
                            autocommit = True,
                            dag = dag)
task2 = PythonOperator(task_id = 'csv_to_database',
                        provide_context = True,
                        python_callable = csv_to_postgres,
                        dag = dag)

task1>>task2