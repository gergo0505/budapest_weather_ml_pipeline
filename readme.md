This project is an automated data engineering pipeline, that extracts daily weather data for Budapest,
processes it in the cloud (AWS S3), and uses a machine learning model to predict tomorrow's temperature.

Project Architecture:
-dags/: daily_weather_pipeline.py: airflow manager
-scripts/:
    -clean_history.py: used to prepare the raw historical weather dataset used for training
    -train_model.py: trains the linear regression model
    -extract_to_s3.py: daily API ingestion
    -transform_s3_data.py: transforming the daily weather data into usable form
    -cloud_predict.py: generates prediction for "tomorrow", storing results in the prediction_tracker.csv
    -update_master_csv.py: updates the master dataset with the daily weather data for later training
-data/: csv files used for training and prediction
-models/: the weather_model.pkl file
-automation/: .bat files for windows task scheduler integration

The training logic:
To train the AI, I shifted the historical temperature data by -1 day, allowing the model to correlate
today's conditions with tomorrow's outcome.

How to run locally (windows machine instructions):

1. Clone the Repo:
git clone https://github.com/yourusername/weather-ai-pipeline.git

2. Set up the Linux Environment (WSL):
Apache Airflow requires a Linux environment. From an Ubuntu/WSL terminal, navigate to where you cloned
the project, and create a virtual environment:

cd /mnt/c/Users/YOUR_USERNAME/path_to_project
python3 -m venv airflow_env
source airflow_env/bin/activate
pip install -r requirements.txt

3. Configure AWS:
To run this pipeline, you will need you own AWS environment
**Create an S3 Bucket
**Create an IAM User
**Generate an Access Key ID, and a Secret Access Key
**Replace the blank spaces in the Python scripts with your credentials

4. Configure Airflow on WSL (Windows):
While still in the activated airflow_env in Ubuntu, set up the airflow database.

export AIRFLOW_HOME=~/airflow
airflow standalone

5. Automate with windows task scheduler:
To automate the pipeline, you can use the windows task scheduler
to run start_airflow.bat and stop_airflow.bat. Run these 5 minutes
before and after the time specified in the dag file.
**You must edit the bat files and insert your username in the appropriate fields.
