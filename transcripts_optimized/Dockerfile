FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ❌ REMOVE sentence-transformers download (no longer needed)
# RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# Copy application code
COPY logging_config.py .
COPY transcripts_src1.py .
COPY vs_pipeline1.py .
COPY dump_db.py .
COPY start.sh .
COPY data/cik_ticker_map.json ./data/

RUN mkdir -p transcripts_output logs && \
    chmod +x start.sh

ENTRYPOINT ["./start.sh"]
