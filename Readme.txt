Stock_market_Realtime_data_processing:
-------------------------------------

indexProcessed.csv: contains the stock market data which was given as input.
Producer.py: Script which read the data from indexProcessed.csv and writes into kafka topic.
Consumer.py: Script which subscribes to kafka topic, reads the data and store in AWS S3.
airflow_producer.py: consists of producer.py script which was automated to run as a batch load by using Airflow.
airflow_consumer.py: consists of consumer.py script which was automated to run as a batch load by using Airflow.