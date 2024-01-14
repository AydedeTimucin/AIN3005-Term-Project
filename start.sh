#!/bin/bash

# Start Kafka Consumer Scripts in the background
python reservation_consumer.py &
python data_sync_consumer.py &
python overdue_books_consumer.py &

# Start Flask App
python app.py
