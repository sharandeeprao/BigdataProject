from kafka import KafkaConsumer
import json

# Define the Kafka server
bootstrap_servers = 'localhost:9092'

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    group_id='test-group',  # Consumer group ID
    enable_auto_commit=True,  # Commit offsets automatically
)

print("Consumer started. Reading messages from 'test-topic'...")

# Read messages from the topic
try:
    for message in consumer:
        print(f"Received message: {message.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
