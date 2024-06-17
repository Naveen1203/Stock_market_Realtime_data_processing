from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime

def task2():
    consumer = KafkaConsumer(
        'airflow_demo',
        bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    s3 = S3FileSystem()
    for count, i in enumerate(consumer):
        with s3.open("s3://kafka-input/stock_market_{}.json".format(count), 'w') as file:
            json.dump(i.value, file)

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
    'task2',
    default_args=default_args,
    description='task2!',
    schedule=timedelta(days=1),
)
task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)