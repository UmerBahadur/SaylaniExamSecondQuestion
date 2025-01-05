from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
import pandas as pd
from io import StringIO
from airflow.operators.bash import BashOperator
import base64
import requests
import boto3
from datetime import datetime
from airflow.utils.dates import days_ago
import logging

# Initialize AWS clients
s3 = boto3.client('s3',
    aws_access_key_id='AKIA',
    aws_secret_access_key="rYYsR8yIcNBX9",
    region_name='us-east-1')
s3_resource = boto3.resource('s3')
glue_client = boto3.client('glue')

# S3 and Glue configurations
BUCKET_NAME = 'secondprojectraw'
RAW_DATA_PREFIX = 'raw/'
TRANSFORMED_DATA_PREFIX = 'transformed/'
PROCESSED_DATA_PREFIX = 'processed/'
CRAWLER_NAME = 'examsecond'

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to process album data
def album(data):
    album_list = []
    for row in data['items']:
        album_info = row['track']['album']
        album_list.append({
            'album_id': album_info['id'],
            'name': album_info['name'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'url': album_info['external_urls']['spotify']
        })
    return album_list

# Function to process artist data
def artist(data):
    artist_list = []
    for row in data['items']:
        for artist in row['track']['artists']:
            artist_list.append({
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            })
    return artist_list

# Function to process song data
def songs(data):
    song_list = []
    for row in data['items']:
        song_info = row['track']
        song_list.append({
            'song_id': song_info['id'],
            'song_name': song_info['name'],
            'duration_ms': song_info['duration_ms'],
            'url': song_info['external_urls']['spotify'],
            'popularity': song_info['popularity'],
            'song_added': row['added_at'],
            'album_id': song_info['album']['id'],
            'artist_id': song_info['album']['artists'][0]['id']
        })
    return song_list

# Task: Process raw data and save transformed data
def transform_data():
    for file in s3.list_objects(Bucket=BUCKET_NAME, Prefix=RAW_DATA_PREFIX)['Contents']:
        file_key = file['Key']
        if file_key.endswith('.json'):
            # Read raw data from S3
            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            data = json.loads(response['Body'].read())
            
            # Process data
            album_df = pd.DataFrame(album(data)).drop_duplicates(subset=['album_id'])
            artist_df = pd.DataFrame(artist(data)).drop_duplicates(subset=['artist_id'])
            song_df = pd.DataFrame(songs(data))
            
            # Convert date columns
            album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
            song_df['song_added'] = pd.to_datetime(song_df['song_added'])
            
            # Save transformed data to S3
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            for name, df in [('songs', song_df), ('album', album_df), ('artist', artist_df)]:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                s3.put_object(Bucket=BUCKET_NAME, Key=f'{TRANSFORMED_DATA_PREFIX}{name}_data/{name}_transformed_{timestamp}.csv', Body=buffer.getvalue())
            
            # Move processed file to "processed" folder
            s3_resource.meta.client.copy(
                {'Bucket': BUCKET_NAME, 'Key': file_key},
                BUCKET_NAME, f'{PROCESSED_DATA_PREFIX}{file_key.split("/")[-1]}'
            )
            s3_resource.Object(BUCKET_NAME, file_key).delete()

# Task: Trigger Glue Crawler
def trigger_glue_crawler():
    response = glue_client.start_crawler(Name=CRAWLER_NAME)
    print(f"Glue Crawler triggered: {response}")

# Define the DAG
dag = DAG(
    'spotify_transform__airflow_dag',
    default_args=default_args,
    description='ETL pipeline for Spotify data using Airflow',
    schedule_interval=None,  # Manual trigger for testing
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Define Airflow tasks
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

glue_crawler_task = PythonOperator(
    task_id='trigger_glue_crawler',
    python_callable=trigger_glue_crawler,
    dag=dag,
)

# Task dependencies
transform_task >> glue_crawler_task