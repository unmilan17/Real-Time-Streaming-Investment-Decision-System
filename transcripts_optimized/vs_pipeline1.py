import os
import backoff
import uuid
from tqdm import tqdm
import google.generativeai as genai
from langchain_text_splitters import RecursiveCharacterTextSplitter
from datetime import datetime
import json
import re
from typing import List, Dict, Any

from logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)

# Configure Gemini
_GEMINI_MODEL = genai.GenerativeModel("gemini-2.5-flash")


def gemini_token_count(string: str) -> int:
    """Count tokens with error handling."""
    try:
        response = _GEMINI_MODEL.count_tokens(string)
        return response.total_tokens
    except Exception as e:
        logger.warning("token_count_error", error=str(e))
        # Return conservative estimate: ~4 chars per token
        return len(string) // 4


class GeminiClient:
    def __init__(self, model_name: str, api_key: str | None = None) -> None:
        if api_key:
            genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.logger = get_logger(f"{__name__}.GeminiClient")

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def create_message(self, system_prompt: str, message: dict) -> str:
        try:
            user_content = message.get("content", "")
            prompt = system_prompt + "\n\n---\n\nUSER CONTENT:\n" + user_content
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            self.logger.warning("create_message_error", error=str(e), action="retrying")
            raise e


class LLMSemanticChunker:
    def __init__(self, api_key: str = None, model_name: str = None):
        if model_name is None:
            model_name = "gemini-2.5-flash"
        self.client = GeminiClient(model_name, api_key=api_key)
        self.logger = get_logger(f"{__name__}.LLMSemanticChunker")

        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=50,
            chunk_overlap=0,
            separators=["\n\n", "\n", ".", "?", "!", " ", ""],
        )

    def get_prompt(self, chunked_input: str, current_chunk: int = 0, 
                   invalid_response: List[int] = None) -> List[Dict[str, str]]:
        """Generate prompt for LLM to identify split points."""
        system_content = (
            "You are an assistant specialized in splitting text into thematically consistent sections. "
            "The text has been divided into chunks, each marked with <|start_chunk_X|> and <|end_chunk_X|> tags, "
            "where X is the chunk number. Your task is to identify the points where splits should occur, "
            "such that consecutive chunks of similar themes stay together. "
            "Respond with a list of chunk IDs where you believe a split should be made. "
            "For example, if chunks 1 and 2 belong together but chunk 3 starts a new topic, "
            "you would suggest a split after chunk 2. THE CHUNKS MUST BE IN ASCENDING ORDER. "
            "Your response MUST be in the form: 'split_after: 3, 5, 8' (or just one number if only one split)."
        )

        user_content = (
            "CHUNKED_TEXT: " + chunked_input + 
            "\n\nRespond only with the IDs of the chunks where you believe a split should occur. "
            "YOU MUST RESPOND WITH AT LEAST ONE SPLIT. "
            f"THESE SPLITS MUST BE IN ASCENDING ORDER AND EQUAL OR LARGER THAN: {current_chunk}."
        )
        
        if invalid_response:
            user_content += (
                f"\n\nThe previous response of {invalid_response} was invalid. "
                "DO NOT REPEAT THIS ARRAY OF NUMBERS. Please try again."
            )

        return [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ]

    def extract_split_numbers(self, result_string: str) -> List[int]:
        """Extract split numbers from LLM response with robust error handling."""
        try:
            # Look for lines containing 'split_after:'
            split_lines = [line for line in result_string.split("\n") 
                          if "split_after:" in line.lower()]
            
            if not split_lines:
                self.logger.warning("split_extraction_failed", reason="no_split_after_found", response_preview=result_string[:100])
                return []
            
            # Extract all numbers from the line
            numbers = re.findall(r'\d+', split_lines[0])
            numbers = list(map(int, numbers))
            
            return numbers
        except Exception as e:
            self.logger.error("split_extraction_error", error=str(e), response_preview=result_string[:100])
            return []

    def split_text(self, text: str) -> List[str]:
        """Split text into semantically coherent chunks using LLM."""
        chunks = self.splitter.split_text(text)
        self.logger.info("initial_split", micro_chunk_count=len(chunks))

        if len(chunks) <= 4:
            self.logger.info("text_too_short", action="returning_single_chunk")
            return [text]

        split_indices = []
        current_chunk = 0
        max_token_window = 800
        max_retries = 3

        with tqdm(total=len(chunks), desc="Semantic chunking") as pbar:
            while current_chunk < len(chunks) - 4:
                # Build context window
                token_count = 0
                chunked_input = ""
                window_end = current_chunk

                for i in range(current_chunk, len(chunks)):
                    chunk_tokens = gemini_token_count(chunks[i])
                    if token_count + chunk_tokens > max_token_window:
                        break
                    token_count += chunk_tokens
                    chunked_input += f"<|start_chunk_{i+1}|>{chunks[i]}<|end_chunk_{i+1}|>"
                    window_end = i

                if window_end == current_chunk:
                    # Single chunk too large, force split
                    self.logger.warning("chunk_too_large", chunk_index=current_chunk, action="forcing_split")
                    split_indices.append(current_chunk + 1)
                    current_chunk += 1
                    pbar.update(1)
                    continue

                # Get LLM decision with retry logic
                messages = self.get_prompt(chunked_input, current_chunk)
                numbers = []
                
                for attempt in range(max_retries):
                    try:
                        [message] = messages[1:]
                        result_string = self.client.create_message(
                            messages[0]["content"], message
                        )
                        
                        numbers = self.extract_split_numbers(result_string)
                        
                        # Validate response
                        if not numbers:
                            self.logger.debug("llm_response_invalid", attempt=attempt + 1, reason="no_valid_splits")
                            if attempt < max_retries - 1:
                                messages = self.get_prompt(chunked_input, current_chunk, [])
                                continue
                            else:
                                # Force a split at midpoint
                                numbers = [current_chunk + (window_end - current_chunk) // 2]
                                self.logger.info("forcing_midpoint_split", split_at=numbers[0])
                                break
                        
                        # Check if numbers are valid
                        if (numbers != sorted(numbers) or 
                            any(num < current_chunk for num in numbers) or
                            any(num > window_end for num in numbers)):
                            self.logger.debug("llm_response_invalid", attempt=attempt + 1, reason="invalid_split_points", splits=numbers)
                            if attempt < max_retries - 1:
                                messages = self.get_prompt(chunked_input, current_chunk, numbers)
                                continue
                            else:
                                # Use first valid number or force split
                                valid_nums = [n for n in numbers if current_chunk <= n <= window_end]
                                numbers = valid_nums if valid_nums else [current_chunk + 1]
                                break
                        
                        # Valid response
                        break
                        
                    except Exception as e:
                        self.logger.warning("llm_attempt_failed", attempt=attempt + 1, error=str(e))
                        if attempt == max_retries - 1:
                            # Force split on final failure
                            numbers = [current_chunk + 1]

                # Add validated splits
                split_indices.extend(numbers)
                current_chunk = numbers[-1] if numbers else current_chunk + 1
                
                # Update progress
                pbar.update(current_chunk - pbar.n)

        # Convert to 0-indexed split points
        chunks_to_split_after = [i - 1 for i in split_indices if i > 0]

        # Merge chunks according to split points
        docs = []
        current_doc = ""
        
        for i, chunk in enumerate(chunks):
            current_doc += chunk + " "
            if i in chunks_to_split_after or i == len(chunks) - 1:
                docs.append(current_doc.strip())
                current_doc = ""

        self.logger.info("semantic_chunking_complete", final_chunk_count=len(docs))
        return docs


class DocumentMetadataBuilder:
    def __init__(self, embed_model_name: str = "models/text-embedding-004"):
        self.logger = get_logger(f"{__name__}.DocumentMetadataBuilder")
        self.logger.info("embedding_model_initialized", model=embed_model_name)
        self.embed_model_name = embed_model_name

    def _validate_metadata(self, metadata: dict, required_fields: list[str]) -> None:
        missing = [field for field in required_fields if field not in metadata]
        if missing:
            raise ValueError(f"Missing required metadata fields: {missing}")

    def _validate_docs(self, docs: list[str]) -> None:
        if not isinstance(docs, list):
            raise TypeError("`docs` must be a list of text chunks.")
        if not all(isinstance(chunk, str) for chunk in docs):
            raise TypeError("Each element in `docs` must be a string.")
        if len(docs) == 0:
            raise ValueError("`docs` cannot be empty.")

    def create_earnings_transcript_json(
        self, docs: List[str], metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Create structured JSON documents with embeddings."""
        required_fields = ["date", "ticker", "year", "quarter", "cik"]
        self._validate_metadata(metadata, required_fields)
        self._validate_docs(docs)

        try:
            datetime.strptime(metadata["date"], "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format. Expected 'YYYY-MM-DD'.")

        doc_id = metadata.get("doc_id") or str(uuid.uuid4())
        source_file = metadata.get(
            "source_file",
            f"{metadata['ticker']}_{metadata['year']}_Q{metadata['quarter']}.json",
        )
        doc_type = metadata.get("doc_type", "transcript")

        json_chunks = []
        self.logger.info("generating_embeddings", chunk_count=len(docs))

        for idx, chunk in enumerate(docs, start=1):
            try:
                embed_response = genai.embed_content(
                    model=self.embed_model_name,
                    content=chunk,
                )
                embed = embed_response["embedding"]
            except Exception as e:
                self.logger.error("embedding_failed", chunk_index=idx, error=str(e))
                raise RuntimeError(f"Embedding failed for chunk {idx}: {e}")

            entry = {
                "metadata": {
                    "date": metadata["date"],
                    "ticker": metadata["ticker"].upper(),
                    "year": int(metadata["year"]),
                    "quarter": int(metadata["quarter"]),
                    "chunk_index": idx,
                    "cik": str(metadata["cik"]).zfill(10),
                    "doc_id": doc_id,
                    "doc_type": doc_type,
                    "source_file": source_file,
                },
                "chunk": chunk.strip(),
                "embedding": embed,
            }
            json_chunks.append(entry)

        return json_chunks


# ------------------- EXECUTION SCRIPT -------------------
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

if __name__ == "__main__":
    input_path = "transcripts_output/AAPL_2024_Q3.json"

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    text = data.get("transcript", "")
    if not text:
        raise ValueError("No transcript text found in the JSON file!")

    metadata = {
        "date": data.get("date", "2024-08-01"),
        "ticker": data.get("ticker", "AAPL"),
        "year": data.get("year", 2024),
        "quarter": data.get("quarter", 3),
        "cik": data.get("cik", "0000320193"),
        "source_file": data.get("source_file", os.path.basename(input_path)),
        "doc_type": "transcript",
    }

    logger.info("starting_semantic_chunking", input_file=input_path)
    api_key = os.getenv("GEMINI_API_KEY")
    chunker = LLMSemanticChunker(api_key=api_key)
    docs = chunker.split_text(text)
    logger.info("chunking_completed", chunk_count=len(docs))

    builder = DocumentMetadataBuilder()
    json_data = builder.create_earnings_transcript_json(docs, metadata)

    output_path = input_path.replace(".json", "_chunks.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    logger.info("output_created", output_file=output_path)