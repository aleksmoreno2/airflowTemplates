
# User behavior analytics
## Capstone project
### Data Engineering Bootcamp

Assume that you work for a user behavior analytics company that collects user data and creates user profiles. You must build a data pipeline to populate the user_behavior_metric table, this is an OLAP table. The data from user_behavior_metric is useful for analysts and tools like dashboard software. 


The table user_behavior_metric takes information from:
A PostgreSQL table named user_purchase.
Daily data by an external vendor in a CSV file named movie_review.csv that populates the classified_movie_review table.


![Logo](https://storage.cloud.google.com/de-bootcamp-am_raw_data/Data_Flow.png)

## References

- [Terraform project setup](https://github.com/aleksmoreno2/DE_Bootcamp_AM)

## Airflow dags

#### DAG - ins_userPurchase_to_postg 
Airflow job to load user_purchase.csv in user_purchase POSTGRES table 

#### DAG - postgres_to_gcs 
Airflow job to export POSTGRES table user_purchase to parquet file in gcs bucket

#### DAG - ins_movieReview_to_raw 
Airflow job to load user_purchase.csv in a parquet file in gcs bucket

#### DAG - dataproc_job_execution 
Airflow job to run classification movie review logic in dataproc cluster

