from datetime import datetime
from airflow import DAG
import os
import requests
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
from airflow.providers.standard.operators.python import PythonOperator

# The official NYC Yellow Taxi data source for 2025
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"


# Download the parquet file and upload it to the MicroCeph bronze layer
def ingest_to_bronze(**kwargs):
    # Download the file
    response = requests.get(TAXI_URL)
    response.raise_for_status()
    
    # Upload to MicroCeph using the Airflow connection
    s3_hook = S3Hook(aws_conn_id='microceph_s3')
    s3_hook.load_file_obj(
        file_obj=BytesIO(response.content),
        key='bronze/yellow_tripdata.parquet',
        bucket_name='owning-the-stack',
        replace=True
    )
    print("Ingestion to bronze complete.")


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def transform_to_silver():
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not access_key or not secret_key:
        raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY must be set in the Airflow task environment.")

    hook = DuckDBHook(
        extensions=["httpfs"],
        autoinstall_extensions=True,
        memory_limit="2GB",
        threads=4,
    )
    connection = hook.get_conn()
    try:
        connection.execute(
            f"""
            CREATE OR REPLACE SECRET microceph_s3 (
                TYPE s3,
                KEY_ID {sql_string(access_key)},
                SECRET {sql_string(secret_key)},
                ENDPOINT 'host.docker.internal:8080',
                URL_STYLE 'path',
                USE_SSL false
            )
            """
        )
        connection.execute(
            """
            COPY (
                SELECT *
                FROM read_parquet('s3://owning-the-stack/bronze/yellow_tripdata.parquet')
                WHERE fare_amount > 0
                  AND passenger_count > 0
            ) TO 's3://owning-the-stack/silver/yellow_tripdata_clean.parquet' (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()

with DAG(
    dag_id='nyc_yellow_taxi_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None, # Trigger DAG manually from UI
    catchup=False,
    tags=['demo', 'microceph', 'duckdb'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_to_bronze',
        python_callable=ingest_to_bronze,
    )

    transform_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )

    ingest_task >> transform_task