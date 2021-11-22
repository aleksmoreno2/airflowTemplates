from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

default_args = {  
    'owner': 'alejandra.moreno',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

CLUSTER_NAME = 'debootcampamcldataproc'
REGION='us-central1'
PROJECT_ID='de-bootcamp-am'
PYSPARK_URI = f"gs://de-bootcamp-am_raw_data/classificationMovieReviewLogic.pyspark.py"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2"
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2"
    }
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

dag = DAG('dataproc-job-execution',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        dag=dag
    )
    
pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,
        dag=dag
    )
    
delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION,
        dag=dag
    )

create_cluster>>pyspark_task>>delete_cluster    
