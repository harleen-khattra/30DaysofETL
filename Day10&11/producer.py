from kafka import KafkaProducer
import csv
import json

# Function to read data from CSV
def read_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

# Function to transform data
def transform_data(data):
    data['processed'] = True  # Add a flag
    data['age'] = int(data['age'])  # Convert age to integer
    return data

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

# Read, transform, and send data to Kafka
for row in read_csv('data.csv'):
    transformed_row = transform_data(row)
    producer.send('etl_topic', value=transformed_row)  # Send to Kafka topic
    print(f"Sent to Kafka: {transformed_row}")

producer.close()  # Close the producer
