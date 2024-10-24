from pyspark.sql import SparkSession
from pyspark.sql.types import *
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import psycopg2
import pandas as pd
from datetime import datetime
import os

def get_schema(table_name):
    artist_schema = StructType([
        StructField("id", StringType(), True),
        StructField("external_urls", StringType(), True),
        StructField("followers", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("popularity", IntegerType(), True),
    ])

    album_schema = StructField([
        StructField("id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("external_urls", StringType(), True),
        StructField("label", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("release_date", StringType(), True),
        StructField("release_date_precision", StringType(), True),
        StructField("total_tracks", IntegerType(), True),
    ])

    track_schema = StructType([
        StructField("id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("disc_number", IntegerType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("external_urls", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("track_number", IntegerType(), True)
    ])

    track_features_schema = StructType([
        StructField("id", StringType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("mode", IntegerType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("valence", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("time_signature", IntegerType(), True)
    ])
    if 'artist' in table_name:
        return artist_schema
    elif 'album' in table_name:
        return album_schema
    elif 'feature' in table_name:
        return track_features_schema
    else:
        return track_schema
    
def extract_postgres_data(table_name):
    conn_params = {
        'dbname': 'spotify',
        'user': 'spotify',
        'password': 'spotify',
        'host': 'localhost',
        'port': '5432'
    }
    conn = psycopg2.connect(**conn_params)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@task
def load_to_hdfs(table_name: str, hdfs_path: str, spark: SparkSession):
    df = extract_postgres_data(table_name)
    schema = get_schema(table_name)

    spark_df = spark.createDataFrame(schema=schema)
    spark_df.write.mode('overwrite').parquet(hdfs_path)
    print(f"Data from {table_name} saved to {hdfs_path}")

@dag(
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 24),
    catchup=False
)
def bronze_layer_dag():
    spark = SparkSession.builder.appName("Bronze Layer").getOrCreate()
    tables = ['artists', 'albums', 'tracks', 'audio_features']
    for table in tables:
        hdfs_path = f"hdfs://namenode:8020/bronze_layer/{table}.parquet"
        load_to_hdfs(table, hdfs_path, spark)
