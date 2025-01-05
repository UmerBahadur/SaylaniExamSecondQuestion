import boto3
import base64
import json
import os
from urllib import request, parse

def lambda_handler(event, context):
    try:
        # Environment variables
        CLIENT_ID = os.environ['CLIENT_ID']
        CLIENT_SECRET = os.environ['CLIENT_SECRET']
        S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
        
        # Spotify API credentials
        SPOTIFY_API_URL = 'https://api.spotify.com/v1/artists/1Xyo4u8uXC1ZmMpatF05PJ'  # Example endpoint
        
        # Fetch Spotify API access token
        token = get_spotify_token(CLIENT_ID, CLIENT_SECRET)
        if not token:
            raise Exception("Failed to fetch access token")
        
        # Fetch data from Spotify API
        data = fetch_spotify_data(SPOTIFY_API_URL, token)
        
        # Save data to S3 bucket
        save_to_s3(data, S3_BUCKET_NAME, 'raw_data.json')
        
        return {
            'statusCode': 200,
            'body': 'Data successfully saved to S3'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

def get_spotify_token(client_id, client_secret):
    """Fetch Spotify API token using client credentials."""
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    }
    data = parse.urlencode({'grant_type': 'client_credentials'}).encode()
    
    req = request.Request(url, data=data, headers=headers)
    with request.urlopen(req) as response:
        if response.status == 200:
            response_data = json.loads(response.read().decode())
            return response_data.get('access_token')
        else:
            raise Exception(f"Failed to fetch token: {response.status}")

def fetch_spotify_data(api_url, token):
    """Fetch data from the Spotify API."""
    headers = {'Authorization': f'Bearer {token}'}
    req = request.Request(api_url, headers=headers)
    with request.urlopen(req) as response:
        if response.status == 200:
            return json.loads(response.read().decode())
        else:
            raise Exception(f"Failed to fetch data: {response.status}")

def save_to_s3(data, bucket_name, file_name):
    """Save data to an S3 bucket."""
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data)
    )
    print(f"Data saved to S3 bucket {bucket_name} with key {file_name}")
