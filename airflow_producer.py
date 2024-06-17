import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime

def task1():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x:
                            dumps(x).encode('utf-8'))
    df = pd.read_csv("/mnt/c/Users/navee/Downloads/indexProcessed.csv")
    while True:
        dict_stock = df.sample(1).to_dict(orient="records")[0]
        producer.send('airflow_demo', value=dict_stock)
        sleep(1)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 05, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'task1',
    default_args=default_args,
    description='task1!',
    schedule=timedelta(days=1),
)
task1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)