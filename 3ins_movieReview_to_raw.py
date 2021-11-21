import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('ins_movieReview_to_raw',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

GOOGLE_CONN_ID = "google_cloud_project",
BUCKET_SRC = "de-bootcamp-am_raw_data",
OBJECT_SRC = "movie_review.csv",
BUCKET_DST = "de-bootcamp-gcs-raw",
OBJECT_DST = "_movie_review.csv"

FILENAME = "user_purchase_"

#Task create_table
ins_moviereview_to_rawbucket = GCSToGCSOperator(
    task_id="copy_movieReview_raw_gcs",
    source_bucket=BUCKET_SRC,
    source_object=OBJECT_SRC,
    destination_bucket=BUCKET_DST,
    destination_object=OBJECT_DST,
    move_object=True,
    dag=dag
)

ins_moviereview_to_rawbucket
