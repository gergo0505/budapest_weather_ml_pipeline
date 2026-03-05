from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# pipeline settings
default_args = {
    'owner': '[USER]',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the schedule
with DAG(
    'daily_budapest_weather_ml',
    default_args=default_args,
    description='Extracts weather, transforms, appends to S3, and predicts tomorrow.',
    schedule='0 11 * * *', 
    catchup=False
) as dag:

    # --- PATH SETUP ---
    PROJECT_DIR = "/mnt/c/Users/[USER]/weather_project"
    
    # use python from the Linux virtual environment
    VENV_PYTHON = f"{PROJECT_DIR}/airflow_env/bin/python"

    # defining the tasks
    extract_task = BashOperator(
        task_id='extract_from_api',
        bash_command=f'cd {PROJECT_DIR} && {VENV_PYTHON} extract_to_s3.py'
    )

    transform_task = BashOperator(
        task_id='transform_and_clean',
        bash_command=f'cd {PROJECT_DIR} && {VENV_PYTHON} transform_s3_data.py'
    )

    append_task = BashOperator(
        task_id='append_to_master_s3',
        bash_command=f'cd {PROJECT_DIR} && {VENV_PYTHON} update_master_csv.py'
    )

    predict_task = BashOperator(
        task_id='predict_and_track',
        bash_command=f'cd {PROJECT_DIR} && {VENV_PYTHON} cloud_predict.py'
    )

    # order of operations
    extract_task >> transform_task >> append_task >> predict_task
