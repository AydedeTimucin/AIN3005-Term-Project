from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'borrow_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Listening for messages
for message in consumer:
    # Simulate sending a notification
    book_id = message.value['book_id']
    user_id = message.value['user_id']
    borrowed_date = message.value['borrowed_date']
    print(f"Notification: User {user_id} borrowed book {book_id} on {borrowed_date}")
