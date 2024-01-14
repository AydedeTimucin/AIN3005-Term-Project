# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the startup script into the container
COPY start.sh /usr/src/app/start.sh

# Make the startup script executable
RUN chmod +x /usr/src/app/start.sh

# Run the startup script when the container launches
CMD ["/usr/src/app/start.sh"]
