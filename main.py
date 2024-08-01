from datetime import datetime
import requests
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

# Константы
API_KEY = 'de88dbfee111dbf031e1a534249a2180'
CITY = 'Saint-Petersburg'
WEATHER_DATA_FILE = '/tmp/weather_data.json'
PROCESSED_DATA_FILE = '/tmp/processed_weather_data.csv'
PARQUET_FILE = '/tmp/weather.parquet'

def download_weather_data():
    url = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'
    response = requests.get(url)
    data = response.json()
    
    with open(WEATHER_DATA_FILE, 'w') as f:
        json.dump(data, f)

def process_weather_data():
    with open(WEATHER_DATA_FILE, 'r') as f:
        data = json.load(f)
    
    temp_kelvin = data['main']['temp']
    temp_celsius = temp_kelvin - 273.15
    
    df = pd.DataFrame({
        'city': [CITY],
        'temperature_celsius': [temp_celsius]
    })
    
    df.to_csv(PROCESSED_DATA_FILE, index=False)

def save_data_to_parquet():
    df = pd.read_csv(PROCESSED_DATA_FILE)
    
    df.to_parquet(PARQUET_FILE)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval='@daily',
)

download_task = PythonOperator(
    task_id='download_weather_data',
    python_callable=download_weather_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_data_to_parquet',
    python_callable=save_data_to_parquet,
    dag=dag,
)

download_task >> process_task >> save_task
