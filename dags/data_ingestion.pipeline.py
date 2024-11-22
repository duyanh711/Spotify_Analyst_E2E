from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawl_data.crawl_data_flow import scrape_data_from_API, load_data_to_db
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
    scrape_data_task = PythonOperator(
        task_id="scrape_data_from_spotify_API",
        python_callable=scrape_data_from_API,
    )

    load_data_task = PythonOperator(
        task_id="load_data_to_db",
        python_callable=load_data_to_db,
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_db,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    scrape_data_task >> load_data_task >> extract_task >> transform_task




