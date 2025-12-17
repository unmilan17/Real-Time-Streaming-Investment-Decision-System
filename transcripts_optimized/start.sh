#!/bin/bash

set -e  # Exit on error

echo "=================================="
echo "Starting Earnings Pipeline"
echo "=================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Validate environment variables
log_info "Validating environment variables..."
if [ -z "$GEMINI_API_KEY" ]; then
    log_error "GEMINI_API_KEY is not set"
    exit 1
fi

if [ -z "$API_NINJAS_API_KEY" ]; then
    log_error "API_NINJAS_API_KEY is not set"
    exit 1
fi

if [ -z "$MONGO_URI" ]; then
    log_warn "MONGO_URI not set, using default: mongodb://localhost:27017/"
    export MONGO_URI="mongodb://localhost:27017/"
fi

# Wait for MongoDB to be ready
log_info "Waiting for MongoDB connection..."
max_attempts=30
attempt=0
until python -c "from pymongo import MongoClient; MongoClient('$MONGO_URI', serverSelectionTimeoutMS=2000).admin.command('ping')" 2>/dev/null; do
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        log_error "MongoDB connection failed after $max_attempts attempts"
        exit 1
    fi
    log_warn "MongoDB not ready, retrying... ($attempt/$max_attempts)"
    sleep 2
done
log_info "MongoDB connection established"

# Step 1: Fetch transcripts
log_info "Step 1: Fetching earnings transcripts..."
START_FETCH=$(date +%s)

if python transcripts_src1.py; then
    END_FETCH=$(date +%s)
    FETCH_TIME=$((END_FETCH - START_FETCH))
    log_info "Transcript fetching completed in ${FETCH_TIME}s"
    
    # Count fetched files
    TRANSCRIPT_COUNT=$(find transcripts_output -name "*.json" -type f 2>/dev/null | wc -l)
    log_info "Fetched $TRANSCRIPT_COUNT transcript files"
    
    if [ "$TRANSCRIPT_COUNT" -eq 0 ]; then
        log_warn "No transcripts fetched. Check API connectivity or configuration."
        exit 0
    fi
else
    log_error "Transcript fetching failed"
    exit 1
fi

echo ""

# Step 2: Process and load to MongoDB
log_info "Step 2: Processing and loading to MongoDB..."
START_PROCESS=$(date +%s)

if python dump_db.py; then
    END_PROCESS=$(date +%s)
    PROCESS_TIME=$((END_PROCESS - START_PROCESS))
    log_info "Processing and loading completed in ${PROCESS_TIME}s"
else
    log_error "Processing and loading failed"
    exit 1
fi

echo ""

# Final summary
TOTAL_TIME=$((END_PROCESS - START_FETCH))
log_info "=================================="
log_info "Pipeline Execution Summary"
log_info "=================================="
log_info "Fetch time:    ${FETCH_TIME}s"
log_info "Process time:  ${PROCESS_TIME}s"
log_info "Total time:    ${TOTAL_TIME}s"
log_info "=================================="

echo ""
log_info "Pipeline completed successfully!"