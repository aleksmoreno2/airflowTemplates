import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('ins_localuserPurchase_to_postg',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def file_path(relative_path):
    dir = "c:"
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_sql')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_sql').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table.
    with open(file_path("user_purchase.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'user_purchases', sep=',')
        get_postgres_conn.commit()


#Task create_table
create_table = PostgresOperator(task_id = 'create_user_purchase_table',
                         sql ="""
                         CREATE TABLE IF NOT EXISTS user_purchases (
                            InvoiceNo VARCHAR(100),
                            StockCode VARCHAR(20),
                            Description VARCHAR(1000),
                            Quantity INT,
                            InvoiceDate TIMESTAMP,
                            UnitPrice NUMERIC(8,3),
                            CustomerID INT,
                            Country VARCHAR(20));
                            """,
                            postgres_conn_id='postgres_sql', 
                            autocommit=True,
                            dag= dag)

#Task populate_table
populate_table = PythonOperator(task_id='csv_to_db',
                   provide_context=True,
                   python_callable=csvToPostgres,
                   dag=dag)

create_table >> populate_table
