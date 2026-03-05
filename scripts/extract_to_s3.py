import requests
import boto3
import json
from datetime import datetime

# --- CONFIGURATION ---
ACCESS_KEY = "[IAM USER ACCESS KEY]"
SECRET_KEY = "[IAM USER SECRET KEY]"
BUCKET_NAME = "[S3 BUCKET]"

# the api for daily weather
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=47.4979&longitude=19.0402&daily=temperature_2m_max,temperature_2m_min,daylight_duration,sunshine_duration,precipitation_sum,wind_speed_10m_max,temperature_2m_mean,cloud_cover_mean,relative_humidity_2m_mean,pressure_msl_mean,weather_code&timezone=Europe%2FBerlin&forecast_days=1"

def upload_weather_to_s3():
    #  Fetch data from the API
    print("Fetching data from Open-Meteo...")
    response = requests.get(API_URL)
    data = response.json()
    
    #  Create a unique filename based on today's date
    today = datetime.now().strftime('%Y-%m-%d')
    filename = f"budapest_weather_{today}.json"
    
    #  Connect to AWS S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    #  Upload the JSON directly to your bucket
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data)
        )
        print(f"Success! {filename} uploaded to {BUCKET_NAME}")
    except Exception as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    upload_weather_to_s3()