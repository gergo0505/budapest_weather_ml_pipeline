import boto3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
import joblib
import io
import os

# --- CONFIG ---
ACCESS_KEY = "[IAM USER ACCES KEY]"
SECRET_KEY = "[IAM USER SECRET_KEY]"
BUCKET_NAME = "[S3 BUCKET NAME]"
MASTER_FILE_KEY = "training_data_clean.csv"
MODEL_KEY = "budapest_weather_model.pkl"

def train_cloud_weather_model():
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    # Download the latest data directly from S3
    print("1. Downloading latest historical data from S3..")
    response = s3.get_object(Bucket=BUCKET_NAME, Key=MASTER_FILE_KEY)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))

    # Shifting tomorrow's mean temp to today since this is what we are trying to predict
    df['target_temp_tomorrow'] = df['mean_temp'].shift(-1)
    df = df.dropna()

    print("Preparing Features (X) and Target (y)..")
    y = df['target_temp_tomorrow']
    X = df.drop(columns=['time', 'target_temp_tomorrow'])

    # Split and Train
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("Training..")
    model = LinearRegression()
    model.fit(X_train, y_train) 

    print("Testing the Model..")
    predictions = model.predict(X_test)
    error = mean_absolute_error(y_test, predictions)
    print(f"Success! On average, the model's prediction is off by {error:.2f}°C.")

    # Save locally temporarily, then push straight to S3
    print("5. Saving and uploading the new Brain to S3...")
    temp_model_path = 'temp_new_model.pkl'
    joblib.dump(model, temp_model_path)
    
    # Overwrite the old model in the bucket
    s3.upload_file(temp_model_path, BUCKET_NAME, MODEL_KEY)
    print("New Model successfully uploaded to S3!")

    # Clean up local files
    if os.path.exists(temp_model_path):
        os.remove(temp_model_path)

if __name__ == "__main__":
    train_cloud_weather_model()