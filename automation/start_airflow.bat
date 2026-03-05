@echo off
echo Starting Airflow Standalone
start "Airflow All-In-One" wsl -d Ubuntu -u [USERNAME] bash -c "source /mnt/c/Users/kanya/weather_project/airflow_env/bin/activate && airflow standalone"