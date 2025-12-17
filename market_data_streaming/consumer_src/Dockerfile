FROM python:3.12-slim

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
COPY process_stream.py .
COPY schemas.py .

# Create output directory
RUN mkdir -p /app/output

# Run the consumer
CMD ["python", "-u", "process_stream.py"]