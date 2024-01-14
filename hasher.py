from werkzeug.security import generate_password_hash

# Hashing a password
hashed_password = generate_password_hash("")

print(hashed_password)

