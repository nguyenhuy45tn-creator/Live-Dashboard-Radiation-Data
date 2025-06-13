import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

TOPIC = 'output_topic'
BOOTSTRAP_SERVERS = 'localhost:29092'
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "default-group")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[Consumer] Listening for messages...")
try:
    for msg in consumer:
        print(f"[Consumer (Topic: {TOPIC})] Received: {msg.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
