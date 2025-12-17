FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script
COPY news-producer-replay.py .

# Create directory for CSV data
RUN mkdir -p /app/data

# Run the producer
CMD ["python", "-u", "news-producer-replay.py"]