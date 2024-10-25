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
    # def fetch_data():
    #     artist_data, albums_data, tracks_data, audio_features_data = scrape_spotify_data()
    #     return artist_data, albums_data, tracks_data, audio_features_data

    fetch_data_task = PythonOperator(
        task_id="fetch_spotify_data",
        python_callable=scrape_data_core_flow,
    )

    # Task 2: Load data into PostgreSQL
    # def load_data(artist_data, albums_data, tracks_data, audio_features_data):
    #     load_to_postgres(artist_data, "artists")
    #     load_to_postgres(albums_data, "albums")
    #     load_to_postgres(tracks_data, "tracks")
    #     load_to_postgres(audio_features_data, "audio_features")

    # load_data_task = PythonOperator(
    #     task_id="load_data_to_postgres",
    #     python_callable=load_data,
    #     op_args=[
    #         "{{ ti.xcom_pull(task_ids='fetch_spotify_data')[0] }}",
    #         "{{ ti.xcom_pull(task_ids='fetch_spotify_data')[1] }}",
    #         "{{ ti.xcom_pull(task_ids='fetch_spotify_data')[2] }}",
    #         "{{ ti.xcom_pull(task_ids='fetch_spotify_data')[3] }}",
    #     ],
    # )

    # Task 3: Extract data from PostgreSQL
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_db,
    )

    # Task 4: Transform data
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    # Define task dependencies
    fetch_data_task >> extract_task >> transform_task




