import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('user_behavior_metric',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


REGION = "us-central1"
PROJECT_ID = "de-bootcamp-am"
BUCKET = 'de-bootcamp-gcs-staging'
#SCHEMA_NAME = "debam_dw"
DATASET_NAME = "debam_dw"
TABLE_NAME = "user_behavior_metric"

# query_userbehavior = (           
       
#     )


create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id = "create_dataset",
            dataset_id = DATASET_NAME,
            project_id = PROJECT_ID,
            location = REGION,
            exists_ok = True,
            dag = dag
        )


create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id = DATASET_NAME,
    project_id = PROJECT_ID,
    table_id = TABLE_NAME,
    exists_ok = True,
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "amount_spent", "type": "DECIMAL", "mode": "REQUIRED"},
        {"name": "review_score", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "insert_date", "type": "DATE", "mode": "REQUIRED"},
    ],
    dag = dag
)


# insert_query_job = BigQueryInsertJobOperator(
#     task_id="insert_query",
#     configuration={
#         "query": {
#             "query": insequery_userbehaviorrt_into_table_query,
#             "useLegacySql": False,
#         }
#     },
#     location=REGION,
#     project_id = PROJECT_ID,
#     dag = dag
# )

#create_dataset >> create_table >> insert_query
create_dataset >> create_table