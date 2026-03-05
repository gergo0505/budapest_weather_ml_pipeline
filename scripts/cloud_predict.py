import boto3
import pandas as pd
import joblib
from datetime import datetime, timedelta
import os
import io

# --- CONFIG ---
ACCESS_KEY = "[IAM USER ACCESS KEY]"
SECRET_KEY = "[IAM USER SECRET KEY]"
BUCKET_NAME = "[S3 BUCKET NAME]" #
MODEL_KEY = "budapest_weather_model.pkl"
TRACKER_KEY = "prediction_tracker.csv"

def predict_weather_from_cloud():
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    # Download the Model.
    print("Downloading the model S3")
    temp_model_path = 'temp_downloaded_model.pkl'
    try:
        s3.download_file(BUCKET_NAME, MODEL_KEY, temp_model_path)
        model = joblib.load(temp_model_path)
    except Exception as e:
        print(f"Failed to download model: {e}")
        return

    # Load today's transformed data.
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    tomorrow_str = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    
    daily_file = f"transformed_{today_str}.csv" 
    print(f"Loading today's weather data from {daily_file}...")
    try:
        df_today = pd.read_csv(daily_file)
    except FileNotFoundError:
        print(f"Could not find {daily_file}.")
        return

    # Predict tomorrow's Temp
    X_today = df_today.drop(columns=['time'])
    prediction = model.predict(X_today)[0]
    actual_today_mean = df_today['mean_temp'].iloc[0]
    
    print(f"Prediction for {tomorrow_str}: {prediction:.2f}°C")

    # 4. Tracking results
    print("\n Updating the Prediction Tracker in S3")
    
    # Download the existing tracker, or make a new one.
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=TRACKER_KEY)
        tracker_df = pd.read_csv(io.BytesIO(response['Body'].read()))
    except Exception:
        print("Tracker not found. Creating a new one!")
        tracker_df = pd.DataFrame(columns=['target_date', 'predicted_temp', 'actual_temp'])

    # Step A: Update today's row with the actual temperature
    if today_str in tracker_df['target_date'].values:
        tracker_df.loc[tracker_df['target_date'] == today_str, 'actual_temp'] = actual_today_mean
    else:
        # If today isn't in there (like on Day 1), just add it as a baseline
        new_actual = pd.DataFrame([{'target_date': today_str, 'predicted_temp': None, 'actual_temp': actual_today_mean}])
        tracker_df = pd.concat([tracker_df, new_actual], ignore_index=True)

    # Step B: Add tomorrow's row with the new prediction
    new_prediction = pd.DataFrame([{'target_date': tomorrow_str, 'predicted_temp': prediction, 'actual_temp': None}])
    tracker_df = pd.concat([tracker_df, new_prediction], ignore_index=True)

    # Clean up any duplicates in case you ran the script twice today
    tracker_df = tracker_df.drop_duplicates(subset=['target_date'], keep='last')

    # Step C: Upload back to S3
    csv_buffer = io.StringIO()
    tracker_df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=TRACKER_KEY, Body=csv_buffer.getvalue())
    
    print("Tracker updated! Here is the current Report Card:")
    print(tracker_df.tail(3)) # prints the last 3 rows

    # 5. Clean up
    if os.path.exists(temp_model_path):
        os.remove(temp_model_path)

if __name__ == "__main__":
    predict_weather_from_cloud()