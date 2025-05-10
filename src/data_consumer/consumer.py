from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'fast-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fast-group'
)

print("[Consumer] Listening for messages...")
try:
    for message in consumer:
        print(f"[Consumer] Received: {message.value.decode()}")
except KeyboardInterrupt:
    print("Consumer stopped.")
