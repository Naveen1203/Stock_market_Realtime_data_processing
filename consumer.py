from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem

consumer = KafkaConsumer(
    'first_topic',
     bootstrap_servers=['localhost:9092'], 
    value_deserializer=lambda x: loads(x.decode('utf-8')))
s3 = S3FileSystem()
for count, i in enumerate(consumer):
    with s3.open("s3://kafka-input/stock_market_{}.json".format(count), 'w') as file:
        json.dump(i.value, file) 
