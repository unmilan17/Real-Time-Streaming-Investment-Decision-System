import os
import json
import requests
import time
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)

# Load API key from .env
load_dotenv()
API_NINJAS_API_KEY = os.getenv("API_NINJAS_API_KEY")
API_NINJAS_API_URL = "https://api.api-ninjas.com/v1/earningstranscript"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMPANY_JSON_FILE = os.path.join(BASE_DIR, "data", "cik_ticker_map.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "transcripts_output")

# Ensure output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_last_quarter():
    """Returns the last completed calendar quarter and corresponding year."""
    now = datetime.now()
    year = now.year
    current_quarter = (now.month - 1) // 3 + 1  # 1–4

    # Move to previous quarter
    last_quarter = current_quarter - 1
    if last_quarter == 0:
        last_quarter = 4
        year -= 1

    return year, last_quarter


def fetch_earnings_transcript(ticker, year=None, quarter=None, max_retries=5):
    """Fetches transcript data from API Ninjas with exponential backoff retries."""
    if year is None or quarter is None:
        year, quarter = get_last_quarter()

    params = {"ticker": ticker}
    headers = {"X-Api-Key": API_NINJAS_API_KEY}

    fetch_logger = logger.bind(ticker=ticker, year=year, quarter=quarter)
    
    backoff = 1  # seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(API_NINJAS_API_URL, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error"):
                fetch_logger.warning("api_error", error=data["error"])
                return None
            elif isinstance(data, list) and data:
                fetch_logger.debug("transcript_fetched", status="success")
                return data[0]
            elif isinstance(data, dict):
                fetch_logger.debug("transcript_fetched", status="success")
                return data
            else:
                fetch_logger.warning("transcript_not_found")
                return None

        except requests.exceptions.RequestException as e:
            fetch_logger.warning("request_failed", attempt=attempt + 1, error=str(e))
            if attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= 2
            else:
                fetch_logger.error("max_retries_reached")
                return None
        except Exception as e:
            fetch_logger.error("unexpected_error", error=str(e))
            return None


def main():
    with open(COMPANY_JSON_FILE, "r") as f:
        companies_dict = json.load(f)

    companies = [(ticker, cik) for ticker, cik in companies_dict.items()]
    year, quarter = get_last_quarter()

    logger.info("fetching_transcripts", year=year, quarter=quarter, company_count=len(companies))

    max_workers = min(10, len(companies))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {
            executor.submit(fetch_earnings_transcript, ticker, year, quarter): (ticker, cik)
            for ticker, cik in companies
        }

        for future in as_completed(future_to_ticker):
            ticker, cik = future_to_ticker[future]
            try:
                data = future.result()
                if data:
                    # Add CIK, year, and quarter info for completeness
                    data.update({"ticker": ticker, "cik": cik, "year": year, "quarter": quarter})

                    # Save each transcript individually
                    filename = f"{ticker}_{year}_Q{quarter}.json"
                    filepath = os.path.join(OUTPUT_DIR, filename)
                    with open(filepath, "w") as f:
                        json.dump(data, f, indent=2)

                    logger.info("transcript_saved", ticker=ticker, file=filename)
                else:
                    logger.warning("transcript_fetch_failed", ticker=ticker)
            except Exception as e:
                logger.error("processing_error", ticker=ticker, error=str(e))

    logger.info("transcripts_complete", output_dir=OUTPUT_DIR)


if __name__ == "__main__":
    main()
