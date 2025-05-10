import os
import pandas as pd
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")
TOPIC = os.getenv("KAFKA_TOPIC")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 5))
SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", 2))

def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_csv_to_kafka(file_path, topic, chunk_size=100):
    producer = init_kafka_producer()
    header = None

    print(f"Reading CSV in chunks of {chunk_size} rows...")
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        if header is None:
            header = list(chunk.columns)
            print(f"CSV Header: {header}")

        for _, row in chunk.iterrows():
            record = row.to_dict()
            print(f"[Producer] Sending: {record}")
            producer.send(topic, record)
            producer.flush()
            time.sleep(SEND_INTERVAL) 

    print("Finished streaming CSV to Kafka.")
    producer.close()

if __name__ == '__main__':
    stream_csv_to_kafka(CSV_FILE_PATH, TOPIC, CHUNK_SIZE)
