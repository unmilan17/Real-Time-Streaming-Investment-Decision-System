FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script
COPY kafka-producer.py .

# Run the producer
CMD ["python", "-u", "kafka-producer.py"]