import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('ins_userPurchase_to_postg',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

GOOGLE_CONN_ID = "google_cloud_default"
POSTGRES_CONN_ID = "postgres_sql"
bucket_name = "de-bootcamp-am_raw_data"
bucket_file = 'user_purchase.csv'

# def read_file(self, filename):
#     gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_default')
#     gcs_file = gcs_hook.open(filename)
#     contents = gcs_file.read()
#     gcs_file.close()
#     self.response.write(contents)

def download(self, bucket, object, filename=None):
        """
        Get a file from Google Cloud Storage.
        :param bucket: The bucket to fetch from.
        :type bucket: string
        :param object: The object to fetch.
        :type object: string
        :param filename: If set, a local file path where the file should be written to.
        :type filename: string
        """
        service = self.get_conn()
        downloaded_file_bytes = service \
            .objects() \
            .get_media(bucket=bucket, object=object) \
            .execute()

        # Write the file to local file path, if requested.
        if filename:
            write_argument = 'wb' if isinstance(downloaded_file_bytes, bytes) else 'w'
            with open(filename, write_argument) as file_fd:
                file_fd.write(downloaded_file_bytes)

        return downloaded_file_bytes
    
def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_sql')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_sql').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table.
    gcs_hook = GoogleCloudStorageHook(gcp_conn_id=GOOGLE_CONN_ID)
    downloaded_file = gcs_hook.download(bucket_name, bucket_file)
    
    if downloaded_file:
            with open(downloaded_file, 'wb') as file_fd:
                file_fd.write(downloaded_file_bytes)
                
    with open(downloaded_file_bytes, 'rb') as f:
        next(f)
        curr.copy_from(f, 'user_purchase', sep=',')
        get_postgres_conn.commit()

#Task create_table
create_table = PostgresOperator(task_id = 'create_user_purchase_table',
                         sql ="""
                            CREATE TABLE IF NOT EXISTS user_purchase (
                            invoice_number VARCHAR(10) PRIMARY KEY,
                            stock_code VARCHAR(20),
                            detail VARCHAR(1000),
                            quantity INT,
                            invoice_date TIMESTAMP,
                            unit_price NUMERIC(8,3),
                            customer_id INT,
                            country VARCHAR(20));
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
