from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'etl_topic',  # Kafka topic to consume
    bootstrap_servers='localhost:9092',  # Kafka broker
    auto_offset_reset='earliest',  # Start reading from the earliest message
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON data
)

# Consume and process data
print("Consuming data from Kafka...")
for message in consumer:
    print(f"Consumed from Kafka: {message.value}")
    # Additional processing or saving to a database/file can go here
