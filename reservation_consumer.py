from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time

# Kafka consumer configuration
topic = 'reservation_topic'
bootstrap_servers = 'kafka-broker:9092'
max_retries = 5
retry_delay = 10  # seconds

# Function to create Kafka consumer with retry
def create_consumer_with_retry():
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
consumer = create_consumer_with_retry()

# Listening for messages
for message in consumer:
    # Process message
    book_id = message.value['book_id']
    user_id = message.value['user_id']
    borrowed_date = message.value['borrowed_date']
    print(f"Notification: User {user_id} borrowed book {book_id} on {borrowed_date}")
