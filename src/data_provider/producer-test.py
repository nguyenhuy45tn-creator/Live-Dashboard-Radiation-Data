from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'fast-topic'

message_count = 0

try:
    while True:
        start_time = time.time()
        for i in range(100):
            msg = f"Message {message_count + i}"
            producer.send(topic, msg.encode())
            print(f"[Producer] Sent: {msg}")
            time.sleep(1)
        producer.flush()
        message_count += 100
        # Sleep to maintain ~100 messages per second
        elapsed = time.time() - start_time
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer stopped.")
