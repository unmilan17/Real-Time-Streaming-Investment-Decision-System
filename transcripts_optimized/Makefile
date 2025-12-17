.PHONY: build run stop clean logs help

# Default target
help:
	@echo "Available commands:"
	@echo "  make build    - Build the Docker image"
	@echo "  make run      - Start the pipeline (builds if needed)"
	@echo "  make stop     - Stop all containers"
	@echo "  make clean    - Remove containers and volumes"
	@echo "  make logs     - Follow pipeline logs"
	@echo "  make mongo    - Connect to MongoDB shell"
	@echo "  make status   - Show container status"

# Build the Docker image
build:
	@echo "Building Docker image..."
	docker compose build --no-cache

# Run the pipeline
run:
	@echo "Starting pipeline..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found!"; \
		echo "Create .env with:"; \
		echo "  GEMINI_API_KEY=your_key_here"; \
		echo "  API_NINJAS_API_KEY=your_key_here"; \
		exit 1; \
	fi
	docker compose up --build

# Stop containers
stop:
	@echo "Stopping containers..."
	docker compose down

# Clean up everything
clean:
	@echo "Removing containers and volumes..."
	docker compose down -v
	rm -rf transcripts_output/* logs/*

# View logs
logs:
	docker compose logs -f pipeline

# Connect to MongoDB shell
mongo:
	docker exec -it earnings_mongodb mongosh earnings_db

# Show container status
status:
	docker compose ps