import os
import json
import time
from typing import List, Dict, Any, Optional
from pathlib import Path
from pymongo import MongoClient, ASCENDING, errors
from dotenv import load_dotenv
from vs_pipeline1 import LLMSemanticChunker, DocumentMetadataBuilder
import google.generativeai as genai
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

from logging_config import configure_logging, get_logger, configure_worker_logging

# Configure logging at module load
configure_logging()
logger = get_logger(__name__)

load_dotenv()


class MongoDBVectorPipeline:
    """Handles MongoDB connection and operations for earnings transcripts with vector embeddings."""
    
    def __init__(self, db_name: str = "earnings_db", collection_name: str = "transcript_chunks"):
        """
        Initialize MongoDB vector pipeline.
        
        Args:
            db_name: MongoDB database name
            collection_name: MongoDB collection name for chunks with embeddings
        """
        self.db_name = db_name
        self.collection_name = collection_name
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.client: Optional[MongoClient] = None
        self.logger = get_logger(f"{__name__}.MongoDBVectorPipeline")
        
    def __enter__(self):
        """Context manager entry - establish connection."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()
        
    def connect(self) -> None:
        """Establish MongoDB connection with error handling."""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.logger.info("mongo_connected", status="success")
        except errors.ServerSelectionTimeoutError:
            self.logger.error("mongo_connection_failed", reason="server_selection_timeout")
            raise
        except errors.ConfigurationError as e:
            self.logger.error("mongo_connection_failed", reason="configuration_error", error=str(e))
            raise
        except Exception as e:
            self.logger.error("mongo_connection_failed", reason="unexpected_error", error=str(e))
            raise
            
    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.logger.info("mongo_connection_closed")
            
    def ensure_indexes(self) -> None:
        """Create necessary indexes including vector search index."""
        if not self.client:
            raise RuntimeError("MongoDB client not connected")
            
        try:
            db = self.client[self.db_name]
            collection = db[self.collection_name]
            
            # Composite index for transcript chunk identification
            collection.create_index(
                [
                    ("metadata.ticker", ASCENDING),
                    ("metadata.year", ASCENDING),
                    ("metadata.quarter", ASCENDING),
                    ("metadata.chunk_index", ASCENDING)
                ],
                unique=True,
                name="chunk_identification_idx",
                background=True
            )
            
            # Index for CIK lookups
            collection.create_index(
                "metadata.cik",
                name="cik_idx",
                background=True
            )
            
            # Index for date-based queries
            collection.create_index(
                "metadata.date",
                name="date_idx",
                background=True
            )
            
            # Index for ticker searches
            collection.create_index(
                "metadata.ticker",
                name="ticker_idx",
                background=True
            )
            
            # Index for document ID lookups
            collection.create_index(
                "metadata.doc_id",
                name="doc_id_idx",
                background=True
            )
            
            # Compound index for time-series queries
            collection.create_index(
                [
                    ("metadata.ticker", ASCENDING),
                    ("metadata.year", ASCENDING),
                    ("metadata.quarter", ASCENDING)
                ],
                name="time_series_idx",
                background=True
            )
            
            self.logger.info("indexes_created", status="success")
            
        except errors.OperationFailure as e:
            self.logger.warning("index_creation_warning", error=str(e))
        except Exception as e:
            self.logger.error("index_creation_failed", error=str(e))
            raise
    
    def is_already_processed(self, ticker: str, year: int, quarter: int) -> bool:
        """Check if transcript is already in database."""
        if not self.client:
            raise RuntimeError("MongoDB client not connected")
            
        db = self.client[self.db_name]
        collection = db[self.collection_name]
        
        return collection.count_documents({
            "metadata.ticker": ticker,
            "metadata.year": year,
            "metadata.quarter": quarter
        }) > 0
    
    def validate_chunk_document(self, doc: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate chunk document structure.
        
        Args:
            doc: Document to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check basic structure
        if not isinstance(doc, dict):
            return False, "Document is not a dictionary"
            
        # Check required top-level fields
        required_fields = ["metadata", "chunk", "embedding"]
        missing_fields = [f for f in required_fields if f not in doc]
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
            
        # Validate metadata
        metadata = doc.get("metadata", {})
        required_metadata = ["ticker", "year", "quarter", "chunk_index", "doc_id"]
        missing_metadata = [f for f in required_metadata if f not in metadata]
        if missing_metadata:
            return False, f"Missing metadata fields: {', '.join(missing_metadata)}"
            
        # Validate data types
        try:
            if not isinstance(metadata["year"], int):
                return False, f"Invalid year type: {type(metadata['year'])}"
            if not isinstance(metadata["quarter"], int):
                return False, f"Invalid quarter type: {type(metadata['quarter'])}"
            if not (1 <= metadata["quarter"] <= 4):
                return False, f"Invalid quarter value: {metadata['quarter']}"
            if not isinstance(metadata["chunk_index"], int):
                return False, f"Invalid chunk_index type: {type(metadata['chunk_index'])}"
        except KeyError as e:
            return False, f"Missing metadata field during validation: {e}"
            
        # Validate chunk content
        chunk = doc.get("chunk", "")
        if not isinstance(chunk, str):
            return False, f"Invalid chunk type: {type(chunk)}"
        if not chunk.strip():
            return False, "Empty chunk content"
            
        # Validate embedding
        embedding = doc.get("embedding")
        if not isinstance(embedding, list):
            return False, f"Invalid embedding type: {type(embedding)}"
        if len(embedding) == 0:
            return False, "Empty embedding vector"
        if not all(isinstance(x, (int, float)) for x in embedding):
            return False, "Embedding contains non-numeric values"
            
        return True, None
            
    def upsert_chunks(self, chunks: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Upsert chunk documents into MongoDB with comprehensive error handling.
        
        Args:
            chunks: List of chunk documents with embeddings
            
        Returns:
            Dictionary with operation statistics
        """
        if not chunks:
            self.logger.warning("upsert_chunks_empty", chunk_count=0)
            return {"inserted": 0, "updated": 0, "skipped": 0, "total": 0, "errors": 0}
            
        if not self.client:
            raise RuntimeError("MongoDB client not connected")
            
        db = self.client[self.db_name]
        collection = db[self.collection_name]
        
        stats = {
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "total": len(chunks)
        }
        
        for idx, chunk_doc in enumerate(chunks, 1):
            # Validate document
            is_valid, error_msg = self.validate_chunk_document(chunk_doc)
            if not is_valid:
                self.logger.warning("chunk_validation_failed", chunk_index=idx, error=error_msg)
                stats["skipped"] += 1
                continue
                
            metadata = chunk_doc["metadata"]
            
            try:
                # Use unique identifier for upsert
                filter_query = {
                    "metadata.ticker": metadata["ticker"],
                    "metadata.year": metadata["year"],
                    "metadata.quarter": metadata["quarter"],
                    "metadata.chunk_index": metadata["chunk_index"]
                }
                
                result = collection.update_one(
                    filter_query,
                    {"$set": chunk_doc},
                    upsert=True
                )
                
                if result.upserted_id:
                    stats["inserted"] += 1
                elif result.modified_count > 0:
                    stats["updated"] += 1
                    
                # Progress indicator
                if idx % 50 == 0 or idx == stats["total"]:
                    self.logger.debug(
                        "chunk_progress",
                        processed=idx,
                        total=stats["total"],
                        inserted=stats["inserted"],
                        updated=stats["updated"]
                    )
                    
            except errors.DuplicateKeyError:
                self.logger.warning(
                    "chunk_duplicate",
                    ticker=metadata["ticker"],
                    year=metadata["year"],
                    quarter=metadata["quarter"],
                    chunk_index=metadata["chunk_index"]
                )
                stats["skipped"] += 1
            except errors.DocumentTooLarge:
                self.logger.error(
                    "chunk_too_large",
                    chunk_index=idx,
                    ticker=metadata["ticker"],
                    year=metadata["year"],
                    quarter=metadata["quarter"]
                )
                stats["errors"] += 1
            except Exception as e:
                self.logger.error("chunk_processing_error", chunk_index=idx, error=str(e))
                stats["errors"] += 1
                
        self._log_statistics(stats)
        return stats
        
    def _log_statistics(self, stats: Dict[str, int]) -> None:
        """Log operation statistics."""
        self.logger.info(
            "operation_statistics",
            inserted=stats["inserted"],
            updated=stats["updated"],
            skipped=stats["skipped"],
            errors=stats["errors"],
            total=stats["total"]
        )
        
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        if not self.client:
            raise RuntimeError("MongoDB client not connected")
            
        db = self.client[self.db_name]
        collection = db[self.collection_name]
        
        try:
            stats = {
                "total_documents": collection.count_documents({}),
                "indexes": collection.index_information(),
                "database": self.db_name,
                "collection": self.collection_name
            }
            
            # Get sample embedding dimension
            sample = collection.find_one({"embedding": {"$exists": True}})
            if sample and "embedding" in sample:
                stats["embedding_dimension"] = len(sample["embedding"])
            
            return stats
        except Exception as e:
            self.logger.error("collection_stats_error", error=str(e))
            return {}


def process_single_file(args):
    """Process a single transcript file (for parallel execution)."""
    filepath, api_key, mongo_uri, db_name, collection_name = args

    # Initialize logging in worker process
    configure_worker_logging()
    worker_logger = get_logger("worker")
    
    try:
        # Configure Gemini API
        genai.configure(api_key=api_key)

        # Load transcript file
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        ticker = data.get("ticker")
        year = int(data.get("year", 0))
        quarter = int(data.get("quarter", 0))
        
        worker_logger = worker_logger.bind(
            file=filepath.name,
            ticker=ticker,
            year=year,
            quarter=quarter
        )

        # Check if already processed (quick check)
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        db = client[db_name]
        collection = db[collection_name]

        if collection.count_documents({
            "metadata.ticker": ticker,
            "metadata.year": year,
            "metadata.quarter": quarter
        }) > 0:
            client.close()
            worker_logger.info("file_skipped", reason="already_processed")
            return (True, filepath, None, "already_processed")

        client.close()

        # Validate transcript content
        text = data.get("transcript", "")
        if not text or not text.strip():
            worker_logger.warning("file_skipped", reason="no_transcript_text")
            return (False, filepath, "No transcript text found", None)

        # Extract metadata
        metadata = {
            "date": data.get("date"),
            "ticker": ticker,
            "year": year,
            "quarter": quarter,
            "cik": data.get("cik"),
            "source_file": data.get("source_file", filepath.name),
            "doc_type": "transcript",
        }

        # Validate required fields
        required_fields = ["ticker", "year", "quarter"]
        missing_fields = [f for f in required_fields if not metadata.get(f)]
        if missing_fields:
            worker_logger.error("file_failed", reason="missing_metadata", missing_fields=missing_fields)
            return (False, filepath, f"Missing metadata: {', '.join(missing_fields)}", None)

        # Perform semantic chunking
        worker_logger.info("chunking_started")
        chunker = LLMSemanticChunker(api_key=api_key)
        docs = chunker.split_text(text)

        if not docs:
            worker_logger.error("file_failed", reason="chunking_empty")
            return (False, filepath, "Chunking produced no results", None)

        # Generate embeddings
        worker_logger.info("embedding_started", chunk_count=len(docs))
        builder = DocumentMetadataBuilder()
        json_data = builder.create_earnings_transcript_json(docs, metadata)

        if not json_data:
            worker_logger.error("file_failed", reason="empty_chunk_list")
            return (False, filepath, "Empty chunk list generated", None)

        worker_logger.info("file_processed", chunk_count=len(json_data))
        return (True, filepath, json_data, None)

    except Exception as e:
        worker_logger.error("file_failed", error=f"{type(e).__name__}: {e}")
        return (False, filepath, f"{type(e).__name__}: {e}", None)


def main():
    """Main pipeline execution with parallel processing."""
    start_time = time.time()
    
    try:
        logger.info("pipeline_started")
        
        # Validate environment variables
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            logger.error("pipeline_failed", reason="missing_api_key", key="GEMINI_API_KEY")
            return 1
            
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        
        # Setup paths - FIXED: Use correct directory
        current_dir = Path(__file__).resolve().parent
        transcripts_dir = current_dir / "transcripts_output"
        
        logger.info("config_loaded", transcripts_dir=str(transcripts_dir))
        
        # Find all transcript files
        if not transcripts_dir.exists():
            logger.error("pipeline_failed", reason="directory_not_found", path=str(transcripts_dir))
            return 1
            
        json_files = list(transcripts_dir.glob("*.json"))
        
        # Filter out already processed chunk files
        json_files = [f for f in json_files if not f.stem.endswith("_chunks")]
        
        logger.info("files_discovered", file_count=len(json_files))
        
        if not json_files:
            logger.warning("pipeline_complete", reason="no_files_to_process")
            return 0
        
        # Initialize MongoDB and create indexes
        with MongoDBVectorPipeline() as mongo_pipeline:
            logger.info("setting_up_indexes")
            mongo_pipeline.ensure_indexes()
            
            # Prepare arguments for parallel processing
            db_name = mongo_pipeline.db_name
            collection_name = mongo_pipeline.collection_name
            
            process_args = [
                (json_file, api_key, mongo_uri, db_name, collection_name)
                for json_file in json_files
            ]
            
            # Determine optimal worker count
            cpu_count = mp.cpu_count()
            max_workers = min(cpu_count - 1, len(json_files), 4)  # Cap at 4 for API rate limits
            
            logger.info("parallel_processing_started", file_count=len(json_files), worker_count=max_workers)
            
            total_processed = 0
            total_failed = 0
            total_skipped = 0
            total_chunks = 0
            
            # Process files in parallel
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(process_single_file, args): args[0]
                    for args in process_args
                }
                
                for future in as_completed(futures):
                    filepath = futures[future]
                    
                    try:
                        success, processed_filepath, result, skip_reason = future.result()
                        
                        if skip_reason == "already_processed":
                            total_skipped += 1
                            continue
                        
                        if not success:
                            logger.error("file_failed", file=processed_filepath.name, error=result)
                            total_failed += 1
                            continue
                        
                        # Upsert chunks to MongoDB
                        chunks = result
                        stats = mongo_pipeline.upsert_chunks(chunks)
                        
                        total_processed += 1
                        total_chunks += stats["inserted"] + stats["updated"]
                        
                        logger.info(
                            "file_completed",
                            file=processed_filepath.name,
                            inserted=stats["inserted"],
                            updated=stats["updated"]
                        )
                        
                    except Exception as e:
                        logger.error("file_processing_error", file=filepath.name, error=str(e))
                        total_failed += 1
            
            # Get final statistics
            logger.info("retrieving_collection_stats")
            collection_stats = mongo_pipeline.get_collection_stats()
        
        # Final summary
        elapsed_time = time.time() - start_time
        
        logger.info(
            "pipeline_completed",
            elapsed_seconds=round(elapsed_time, 2),
            files_processed=total_processed,
            files_total=len(json_files),
            files_skipped=total_skipped,
            files_failed=total_failed,
            total_chunks=total_chunks,
            total_documents=collection_stats.get("total_documents")
        )
        
        return 0 if total_failed == 0 else 1
        
    except KeyboardInterrupt:
        logger.warning("pipeline_interrupted", reason="keyboard_interrupt")
        return 130
    except Exception as e:
        logger.error("pipeline_fatal_error", error=str(e), exc_info=True)
        return 1


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)