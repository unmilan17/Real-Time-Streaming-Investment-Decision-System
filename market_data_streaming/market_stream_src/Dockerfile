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

# Copy streaming script
COPY market_stream.py .

# Create data directory
RUN mkdir -p /app/data

# Run the streaming script
CMD ["python", "-u", "market_stream.py"]
