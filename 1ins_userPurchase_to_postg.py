import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
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

#def read_file(filename):
#     google_cloud_storage_conn_id='google_cloud_default'
#     gcs_file = gcs.open(filename)
#     contents = gcs_file.read()
#     gcs_file.close()
#     self.response.write(contents)
 
#def __init__(self,
#                 google_cloud_storage_conn_id='google_cloud_default',
#                 delegate_to=None):
#        super(GoogleCloudStorageHook, self).__init__(google_cloud_storage_conn_id,
#                                                     delegate_to)

#def get_conn(self):
#        """
#        Returns a Google Cloud Storage service object.
#        """
#        http_authorized = self._authorize()
#        return build('storage', 'v1', http=http_authorized)    
    
#def download(bucket, object, filename=None):
#        g_hook = GoogleCloudStorageHook(gcp_conn_id=GOOGLE_CONN_ID,)
#        bucket = g_hook.bucket(bucket)
#        blob = bucket.blob(blob_name=object)

#        if filename:
#            blob.download_to_filename(filename)
#            self.log.info('File downloaded to %s', filename)
#            return filename
#        else:
#            return blob.download_as_string()

def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_sql')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_sql').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table.
    g_hook = GoogleCloudStorageHook(gcp_conn_id=GOOGLE_CONN_ID,)
    
    g_hook.download(bucket_name,bucket_file,'file_fd')
    with open(file_fd, 'rb') as f:
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
