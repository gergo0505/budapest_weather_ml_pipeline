import pandas as pd

# loading file, skipping first 3 rows
df_hist = pd.read_csv('historical_weather.csv', skiprows=3)

# renaming columns to match the structure of the daily weather data
column_map = {
    'temperature_2m_max (°C)': 'max_temp',
    'temperature_2m_min (°C)': 'min_temp',
    'temperature_2m_mean (°C)': 'mean_temp',
    'precipitation_sum (mm)': 'precipitation_sum',
    'wind_speed_10m_max (km/h)': 'wind_speed_10m_max',
    'relative_humidity_2m_mean (%)': 'relative_humidity_2m_mean',
    'pressure_msl_mean (hPa)': 'pressure_msl_mean',
    'sunshine_duration (s)': 'sunshine_duration',
    'daylight_duration (s)': 'daylight_duration',
    'cloud_cover_mean (%)': 'cloud_cover_mean'
}

df_hist = df_hist.rename(columns=column_map)

# drop weather code column (it's not needed in this iteration)
if 'weather_code' in df_hist.columns:
    df_hist = df_hist.drop(columns=['weather_code'])

# only keeping the columns we get in the daily weather data csv
final_columns = [
    'time', 'max_temp', 'min_temp', 'daylight_duration', 
    'sunshine_duration', 'precipitation_sum', 
    'wind_speed_10m_max', 'mean_temp', 'cloud_cover_mean',
    'relative_humidity_2m_mean', 'pressure_msl_mean'
]

# only grab succesfully renamed lists
df_hist = df_hist[[c for c in final_columns if c in df_hist.columns]]

# saving the cleaned training data
df_hist.to_csv('training_data_clean.csv', index=False)
print("Historical data cleaned and saved as 'training_data_clean.csv'!")