FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy consumer script
COPY news_consumer.py .

# Create output directory
RUN mkdir -p /app/output

# Run the consumer
CMD ["python", "-u", "news_consumer.py"]