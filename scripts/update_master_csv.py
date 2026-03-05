import boto3
import pandas as pd
import io
from datetime import datetime

# --- CONFIG ---
ACCESS_KEY = "[IAM USER ACCES KEY]"
SECRET_KEY = "[IAM USER SECRET KEY]"
BUCKET_NAME = "[S3 BUCKET NAME]"
MASTER_FILE_KEY = "training_data_clean.csv"

def append_daily_weather():
    # Connect to S3
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    today = datetime.now().strftime('%Y-%m-%d')
    daily_file = f"transformed_{today}.csv"

    try:
        # Download the master dataset from S3 directly into Pandas
        print(f"Downloading {MASTER_FILE_KEY} from S3...")
        response = s3.get_object(Bucket=BUCKET_NAME, Key=MASTER_FILE_KEY)
        master_df = pd.read_csv(io.BytesIO(response['Body'].read()))

        #  Load today's newly transformed data (from your local folder)
        print(f"Loading today's data from {daiy_file}...")
        daily_df = pd.read_csv(daily_file)

        #  THE MERGE: Glue today's data to the bottom of the master dataset
        updated_df = pd.concat([master_df, daily_df], ignore_index=True)

        # Remove duplicates if ran twice
        updated_df = updated_df.drop_duplicates(subset=['time'], keep='last')

        # Convert the updated DataFrame back into a CSV in memory
        csv_buffer = io.StringIO()
        updated_df.to_csv(csv_buffer, index=False)

        # Overwrite the old CSV with the new one
        print("Uploading updated Master Dataset back to S3...")
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=MASTER_FILE_KEY, 
            Body=csv_buffer.getvalue()
        )
        
        print(f"Success! Master dataset updated. It now has {len(updated_df)} rows.")

    except Exception as e:
        print(f"Failed to update master dataset: {e}")

if __name__ == "__main__":
    append_daily_weather()