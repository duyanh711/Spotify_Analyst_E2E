from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawl_data.crawl_data_flow import scrape_data_core_flow
from dags.extract import extract_data_from_db
from dags.transform import transform

default_args = {
    "owner": "Smart Data Squad",
    "start_date": datetime(2024, 10, 25)
}

with DAG(
    "spotify_data_ingestion_pipeline",
    default_args=default_args,
    description="Spotify Data Ingestion Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 25),
    catchup=False
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_spotify_data",
        python_callable=scrape_data_core_flow,
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_db,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    # fetch_data_task >>
    extract_task >> transform_task




