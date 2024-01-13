from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'overdue_books_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    overdue_info = message.value
    print(f"Overdue book alert: Book ID {overdue_info['book_id']} is overdue. User ID: {overdue_info['user_id']}")
