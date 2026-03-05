@echo off
echo Shutting down Airflow windows...
wsl -d Ubuntu -u [USERNAME] bash -c "pkill -f airflow"
echo Airflow has been shut down.
timeout /t 3