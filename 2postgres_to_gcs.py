import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('postgres_to_gcs',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

GOOGLE_CONN_ID = "google_cloud_project"
POSTGRES_CONN_ID = "postgres_sql"
FILENAME = "user_purchase_"
SQL_QUERY = "select * from user_purchase"
bucket_name = "de-bootcamp-gcs-raw"

upload_data = PostgresToGCSOperator(
        task_id="get_data", 
        sql=SQL_QUERY, 
        bucket=bucket_name, 
        filename=FILENAME, 
        gzip=False, 
        dag=dag)
        
upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="get_data_with_server_side_cursor",
        sql=SQL_QUERY,
        bucket=bucket_name,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
        export_format='parquet',
        dag=dag)

upload_data >> upload_data_server_side_cursor
