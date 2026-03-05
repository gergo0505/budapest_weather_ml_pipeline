import boto3
import pandas as pd
import json
from datetime import datetime

# --- CONFIG ---
ACCESS_KEY = "[IAM USER ACCES KEY]"
SECRET_KEY = "[IAM USER SECRET KEY]"
BUCKET_NAME = "[S3 BUCKET NAME]"

def transform_latest_weather():
    # Connect to S3
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    # Find today's file
    today = datetime.now().strftime('%Y-%m-%d')
    filename = f"budapest_weather_{today}.json"
    
    print(f"Reading {filename} from S3...")
    
    try:
        # Get the object from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=filename)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)
        
        df = pd.DataFrame(data['daily'])

        # Drop the weather code
        if 'weather_code' in df.columns:
            df = df.drop(columns=['weather_code'])
        
        # Quick Clean: Rename columns to be more readable
        df = df.rename(columns={
            'temperature_2m_max': 'max_temp',
            'temperature_2m_min': 'min_temp',
            'temperature_2m_mean': 'mean_temp'
        })
        
        print("Data Transformed successfully:")
        print(df)
        
        # Save locally
        df.to_csv(f"transformed_{today}.csv", index=False)
        print(f"Saved to transformed_{today}.csv")

    except Exception as e:
        print(f"Transformation failed: {e}")

if __name__ == "__main__":
    transform_latest_weather()