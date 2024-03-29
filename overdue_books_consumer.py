from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time

def create_consumer_with_retry(topic, bootstrap_servers, max_retries=5, retry_delay=10):
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            return consumer
        except NoBrokersAvailable:
            print(f"Broker not available, retrying {i+1}/{max_retries}...")
            time.sleep(retry_delay)
    raise Exception("Failed to connect to Kafka broker after several retries.")

# Initialize Kafka Consumer with retry mechanism
consumer = create_consumer_with_retry('overdue_books_topic', 'kafka-broker:9092')

for message in consumer:
    overdue_info = message.value
    print(f"Overdue book alert: Book ID {overdue_info['book_id']} is overdue. User ID: {overdue_info['user_id']}")
