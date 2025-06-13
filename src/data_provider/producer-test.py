import json
import os
from kafka import KafkaProducer
import time
from dotenv import load_dotenv
from pprint import pprint

load_dotenv(override=True)

TOPIC = 'input'
BOOTSTRAP_SERVERS ="localhost:29092"

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))


try:
    i = 0
    while True:
        i += 1
        msg = { 'Message' : i}
        producer.send(TOPIC, msg)
        pprint(f"[Producer (Topic: {TOPIC})] Sent: {msg}")
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer stopped.")
