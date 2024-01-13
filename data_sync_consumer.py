from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'data_sync_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def handle_create(message):
    # Logic to handle creation of a new document
    print(f"New document created in {message['collection']} with ID {message['document_id']}")

def handle_update(message):
    # Logic to handle update of a document
    print(f"Document in {message['collection']} with ID {message['document_id']} was updated")

def handle_delete(message):
    # Logic to handle deletion of a document
    print(f"Document in {message['collection']} with ID {message['document_id']} was deleted")


for message in consumer:
    if message.value['operation'] == 'create':
        handle_create(message.value)
    elif message.value['operation'] == 'update':
        handle_update(message.value)
    elif message.value['operation'] == 'delete':
        handle_delete(message.value)