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

def handle_create(message):
    # Logic to handle creation of a new document
    print(f"New document created in {message['collection']} with ID {message['document_id']}")

def handle_update(message):
    # Logic to handle update of a document
    print(f"Document in {message['collection']} with ID {message['document_id']} was updated")

def handle_delete(message):
    # Logic to handle deletion of a document
    print(f"Document in {message['collection']} with ID {message['document_id']} was deleted")

# Initialize Kafka Consumer with retry mechanism
consumer = create_consumer_with_retry('data_sync_topic', 'kafka-broker:9092')

for message in consumer:
    if message.value['operation'] == 'create':
        handle_create(message.value)
    elif message.value['operation'] == 'update':
        handle_update(message.value)
    elif message.value['operation'] == 'delete':
        handle_delete(message.value)
