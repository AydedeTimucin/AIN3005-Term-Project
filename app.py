import random
import json
from datetime import datetime, timedelta
from bson import ObjectId
from flask import Flask, jsonify, request, render_template, redirect, session, url_for
from flask_jwt_extended import JWTManager, verify_jwt_in_request, create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import check_password_hash
from kafka import KafkaProducer
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from functools import wraps


app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'your_jwt_secret_key'
jwt = JWTManager(app)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Update if your Kafka server is on a different address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Establish a connection to the MongoDB database

client = MongoClient('mongodb://tide-project:41eaH06olf2aw7jRV0mxXnZXIA3gkNO8ys38gcNLOYPOfyE0iK5QSrxcEoT9i0OZccLy8CV1kzxfACDbkfJcEg==@tide-project.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@tide-project@')
db = client.library_database
books_collection = db.books
borrowing_records_collection = db.borrowing_records
users_collection = db.users

def determine_due_date(user_id):
    user = users_collection.find_one({"userId": user_id})
    if user and user['role'] == 'faculty':
        return datetime.now() + timedelta(days=30)  # 30 days for faculty
    else:
        return datetime.now() + timedelta(days=15)  # 15 days for others


def can_borrow_more(user_id):
    user = users_collection.find_one({"userId": user_id})
    if not user:
        return False

    max_books_allowed = 5 if user['role'] == 'faculty' else 3
    borrowed_books_count = borrowing_records_collection.count_documents({"userId": user_id, "status": "borrowed"})

    return borrowed_books_count < max_books_allowed

def check_overdue_books():
    current_time = datetime.now()
    overdue_books = borrowing_records_collection.find({"dueDate": {"$lt": current_time}, "status": "borrowed"})
    for book in overdue_books:
        overdue_message = {
            'book_id': book['bookId'],
            'user_id': book['userId'],
            'due_date': book['dueDate']
        }
        producer.send('overdue_books_topic', value=overdue_message)


def jwt_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Retrieve the token from the session
        token = session.get('jwt_token')
        if token:
            try:
                # Manually set the JWT token for verification
                with app.test_request_context(headers={'Authorization': f'Bearer {token}'}):
                    verify_jwt_in_request()
                return f(*args, **kwargs)
            except:
                # Handle invalid token or other verification errors
                return jsonify({"msg": "Invalid token"}), 401
        else:
            # If no token in session, redirect to login or return error
            return jsonify({"msg": "Missing token"}), 401
    return decorated_function

scheduler = BackgroundScheduler()
scheduler.add_job(func=check_overdue_books, trigger="interval", hours=12)
scheduler.start()




@app.route('/')
def home():
    return render_template('index.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form["username"]
    password = request.form["password"]
    user = users_collection.find_one({"username": username})

    if user and check_password_hash(user['password'], password):
        access_token = create_access_token(identity={"userId": user['userId'], "role": user['role']})
        session['jwt_token'] = access_token  # Set the token in the session
        return redirect(url_for('home'))  # Redirect to the home page
    else:
        return jsonify({"msg": "Invalid username or password"}), 401
    

@app.route('/protected-route', methods=['GET'])
@jwt_required_custom()
def protected():
    try:
        current_user = get_jwt_identity()
        return jsonify(logged_in_as=current_user), 200
    except:
        return redirect(url_for('login'))


@app.route('/books/search', methods=['GET'])
def search_books():
    search_query = request.args.get('query')
    matching_books = books_collection.find({"title": {"$regex": search_query, "$options": "i"}})

    # Convert MongoDB documents to JSON serializable format
    def convert_to_json(doc):
        doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
        return doc

    serializable_books = [convert_to_json(book) for book in matching_books]
    return jsonify({"books": serializable_books}), 200


@app.route('/books/reserve', methods=['POST'])
@jwt_required_custom()
def reserve_book():
    current_user = get_jwt_identity()

    book_id = request.form('book_id')
    user_id = current_user['user_id']
    
    # Logic to reserve the book in the database
    # Update the book's availability
    books_collection.update_one({"id": str(book_id)}, {"$set": {"available": False}})

    data_sync_message = {
        'operation': 'update',
        'collection': 'books',
        'document_id': book_id,
        'updated_fields': {'available': False}
    }
    producer.send('data_sync_topic', value=data_sync_message)

    # Send reservation message to Kafka
    reservation_message = {
        'book_id': book_id,
        'user_id': user_id
    }
    producer.send('reservation_topic', value=reservation_message)

    return jsonify({"message": "Book reserved successfully"}), 200


@app.route('/books/borrow', methods=['POST'])
@jwt_required_custom()
def borrow_book():
    current_user = get_jwt_identity()
    user_role = current_user["role"]

    try:    
        book_id = request.form.get('book_id')

        book = books_collection.find_one({"id": str(book_id), "available": True})

        if not current_user or not book:
            return jsonify({"message": "User or book not found"}), 404

        if book['type'] == 'Periodical':
            return jsonify({"message": "Periodicals cannot be borrowed"}), 400

        if book['type'] == 'Textbook' and user_role != 'Faculty':
            return jsonify({"message": "Only faculty members can borrow textbooks"}), 400
        
        if not can_borrow_more(current_user["user_id"]):
            return jsonify({"message": "Borrowing limit reached"}), 400

        if book:
            # Update the book's availability
            books_collection.update_one({"id": str(book_id)}, {"$set": {"available": False}})
            
            data_sync_message = {
                'operation': 'update',
                'collection': 'books',
                'document_id': book_id,
                'updated_fields': {'available': False}
            }
            producer.send('data_sync_topic', value=data_sync_message)

            # Create a new borrowing record
            borrowing_record = {
                "recordId": "r" + str(random.randint(1000, 9999)),
                "bookId": book_id,
                "userId": current_user["user_id"],
                "borrowedDate": datetime.now().strftime("%Y-%m-%d"),
                "dueDate": determine_due_date(current_user["user_id"]).strftime("%Y-%m-%d"),
                "status": "borrowed"
            }
            borrowing_records_collection.insert_one(borrowing_record)

            data_sync_message_create = {
                'operation': 'create',
                'collection': 'borrowing_records',
                'document_id': borrowing_record['recordId'],
                'new_document': borrowing_record
            }
            producer.send('data_sync_topic', value=data_sync_message_create)

            message = {
                'book_id': book_id,
                'user_id': current_user["user_id"],
                'borrowed_date': datetime.now().strftime("%Y-%m-%d")
            }
            producer.send("borrow_topic", value=message)
            return jsonify({"message": f"Book ID {book_id} borrowed successfully"}), 200
        else:
            return jsonify({"message": "Book not available or not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/books/return', methods=['POST'])
@jwt_required_custom()
def return_book():
    book_id = request.form.get('book_id')

    borrowing_record = borrowing_records_collection.find_one({"bookId": book_id, "status": "borrowed"})
    if borrowing_record:
        books_collection.update_one({"id": str(book_id)}, {"$set": {"available": True}})
        borrowing_records_collection.delete_one({"_id": borrowing_record['_id']})
        
        data_sync_message_delete = {
            'operation': 'delete',
            'collection': 'borrowing_records',
            'document_id': borrowing_record['recordId'],
        }
        producer.send('data_sync_topic', value=data_sync_message_delete)

        return jsonify({"message": f"Book ID {book_id} returned successfully"}), 200
    else:
        return jsonify({"message": "No active borrowing record found for this book"}), 404


if __name__ == '__main__':
    app.run(debug=True)
