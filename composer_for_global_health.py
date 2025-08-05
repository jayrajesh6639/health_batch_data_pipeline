from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args={
   'owner':'airflow',
   'depend_on_past':False,
   'email_on_failure':False,
   'email_on_retry':False,
   'retries':1,
   }
project_id='fleet-reserve-464105-i4'
staging_dataset_id='global_stg_ds'
transform_dataset_id='global_transform_ds'

source_table=f'{project_id}.{staging_dataset_id}.global_stg_tbl'
countries=['USA', 'India', 'Germany']
with DAG(
    dag_id='load_and_transform',
    default_args=default_args,
    description='Load a CSV file from GCS to Bigquery and create specific table',
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 2),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv']
) as dag:

    check_file_exists = GCSObjectExistenceSensor(
        task_id="check_file_exists",
        bucket="health-data03082025",
        object="hospitaldata/global_health_data.csv",
        timeout=300,
        poke_interval=30,
        mode='poke',
    )

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket="health-data03082025",
        source_objects=["hospitaldata/global_health_data.csv"],
        destination_project_dataset_table=source_table,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
    )

    try:
        with open("/home/airflow/gcs/data/replace_file.sql") as f:
            query_str = f.read()
    except Exception as e:
        query_str = ""
        raise RuntimeError(f"Failed to read file:{e}")

    for country in countries:
        query = query_str.format(country=country)
        country_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{str(country).lower()}',
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False
                }
            },
            location='US'
        )

        check_file_exists >> load_csv_to_bigquery >> country_task
