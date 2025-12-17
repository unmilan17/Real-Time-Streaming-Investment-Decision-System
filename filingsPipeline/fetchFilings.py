import os
import time
import json
import logging
import requests
import threading
import random
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()


# ========== Config ==========
ALLOWED_FORMS = {"10-K", "10-Q", "8-K", "4", "13F", "13D", "DEF 14A", "S-1", "20-F", "NT 10-K", "DEFA14A"}

# operational tuning (safe to edit by collaborators)
MAX_FILINGS_PER_TICKER = 5
REQUESTS_PER_SECOND = 5
RETRY_BACKOFF = 2
MAX_WORKERS = 8
MANUAL_MAX_ATTEMPTS = 5

# base URLs and paths (common project settings)
EDGAR_BASE_URL = os.getenv("EDGAR_BASE_URL", "https://data.sec.gov/submissions")
CIK_TICKER_MAP_PATH = "data/cik_ticker_map.json"
CHECKPOINT_PATH = "data/last_processed.json"

# --- SEC compliance: always include a contact email ---
USER_AGENT = os.getenv(
    "EDGAR_USER_AGENT"
)

# ========== Logging ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("edgar_ingest_fixed")


# ========== Utils ==========
def load_json(path: str) -> Dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        logger.exception("Invalid JSON at %s; returning empty dict", path)
        return {}


def atomic_write_json(path: str, obj: Dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
    os.replace(tmp, path)


class HostResolutionError(Exception):
    """Raised when DNS resolution for the target host fails."""


# ========== EdgarIngestor ==========
class EdgarIngestor:
    def __init__(self, rate_limit: int = REQUESTS_PER_SECOND, user_agent: str = USER_AGENT, max_workers: int = MAX_WORKERS):
        self.rate_limit = rate_limit
        self.user_agent = user_agent
        self.lock = threading.Lock()           # protects last_request_time
        self.last_request_time = 0.0
        self.thread_checkpoint_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # build a requests.Session with a Retry adapter (for idempotent GETs)
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            backoff_factor=0.5,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # --- DNS utility ---
    def _host_resolves(self, url: str) -> bool:
        """Return True if host resolves via socket.gethostbyname; False otherwise."""
        try:
            host = url.split("://", 1)[1].split("/", 1)[0]
            # strip possible port
            host = host.split(":", 1)[0]
            socket.gethostbyname(host)
            return True
        except Exception as e:
            logger.debug("Host resolution check failed for host extracted from %s: %s", url, e)
            return False

    # --- rate-limited and robust GET ---
    def _rate_limited_get(self, url: str, timeout: int = 30) -> requests.Response:
        """
        Rate-limited GET which:
        - Enforces per-process rate with a lock.
        - Uses session with Retry adapter for transient HTTP errors.
        - Has a manual retry loop with exponential backoff + jitter to capture DNS/connection errors.
        - Raises HostResolutionError on DNS resolution failure so caller can decide to skip.
        """

        # quick DNS check: if host doesn't resolve, bail early (avoid hammering DNS)
        if not self._host_resolves(url):
            # More diagnostic detail for the logs:
            host = url.split("://", 1)[1].split("/", 1)[0].split(":", 1)[0]
            msg = f"Host '{host}' could not be resolved (DNS). Will not attempt HTTP request."
            logger.error(msg)
            raise HostResolutionError(msg)

        # rate-limiting (thread-safe)
        with self.lock:
            min_interval = 1.0 / float(max(self.rate_limit, 1))
            since = time.time() - self.last_request_time
            if since < min_interval:
                time.sleep(min_interval - since)
            # update last_request_time to now for pacing
            self.last_request_time = time.time()

        backoff = RETRY_BACKOFF
        last_exc = None
        for attempt in range(1, MANUAL_MAX_ATTEMPTS + 1):
            try:
                headers = {"User-Agent": self.user_agent}
                resp = self.session.get(url, headers=headers, timeout=timeout)
            except requests.RequestException as e:
                # Inspect for name resolution specifics (some platforms embed urllib3/requests messages)
                last_exc = e
                msg = str(e)
                logger.warning("Request exception for %s: %s (attempt %d/%d)", url, e, attempt, MANUAL_MAX_ATTEMPTS)

                # if the underlying error indicates DNS resolution specifically, surface a HostResolutionError
                if "NameResolutionError" in msg or "Name or service not known" in msg or isinstance(e, requests.exceptions.ConnectionError) and "Temporary failure in name resolution" in msg:
                    logger.error("Detected DNS resolution error for %s: %s", url, e)
                    raise HostResolutionError(f"DNS resolution failure for host in {url}: {e}")

                # otherwise wait and retry with jitter
                sleep_time = backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                time.sleep(sleep_time)
                continue

            # got a response object; handle status codes
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                # not found — return response and let caller handle it
                return resp
            if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                logger.warning("Received status %s for %s (attempt %d/%d)", resp.status_code, url, attempt, MANUAL_MAX_ATTEMPTS)
                sleep_time = backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                time.sleep(sleep_time)
                continue

            # any other status - raise
            resp.raise_for_status()

        # if we reach here, raise the last exception or a generic RuntimeError
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Failed to GET {url} after {MANUAL_MAX_ATTEMPTS} attempts")

    # --- EDGAR-specific methods ---
    def map_tickers_to_cik(self, tickers: List[str]) -> Dict[str, str]:
        mapping = load_json(CIK_TICKER_MAP_PATH)
        result = {}
        for t in tickers:
            if t in mapping:
                result[t] = mapping[t]
            else:
                logger.error("No CIK found for ticker %s; skipping", t)
        return result

    def get_filing_metadata_for_cik(self, cik: str) -> Dict:
        # Guard: ensure cik can be int
        try:
            cik_int = int(cik)
        except Exception:
            raise ValueError(f"Invalid CIK value: {cik}")

        url = f"{EDGAR_BASE_URL}/CIK{cik_int:010d}.json"
        resp = self._rate_limited_get(url)
        # If 404 -> return empty dict
        if resp.status_code == 404:
            logger.warning("No submissions JSON found for CIK %s (URL %s returned 404)", cik, url)
            return {}
        return resp.json()

    def _normalize_accession(self, accession: str) -> str:
        return accession.replace("-", "")

    def download_filing_html(self, ticker: str, cik: str, accession: str, primary_document: Optional[str] = None) -> str:
        cik_nozeros = str(int(cik))
        acc_nodash = self._normalize_accession(accession)

        # try primary document if provided
        if primary_document:
            url = f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{primary_document}"
            resp = self._rate_limited_get(url)
            if resp.status_code == 200:
                return resp.text
            logger.debug("Primary document URL returned %s for %s", resp.status_code, url)

        # try index.json for folder listing
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/index.json"
        resp = self._rate_limited_get(index_url)
        if resp.status_code == 200:
            try:
                listing = resp.json()
                items = listing.get("directory", {}).get("item", [])
                candidates = [it.get("name") for it in items if isinstance(it.get("name"), str)]
                prioritized = [n for n in candidates if n.lower().endswith((".htm", ".html"))]
                if not prioritized:
                    prioritized = [n for n in candidates if n.lower().endswith((".txt", ".xml"))]

                for name in prioritized:
                    file_url = f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{name}"
                    fresp = self._rate_limited_get(file_url)
                    if fresp.status_code == 200:
                        return fresp.text
                    else:
                        logger.debug("Candidate file %s returned %s", file_url, fresp.status_code)
            except Exception as e:
                logger.exception("Error parsing index.json for %s %s: %s", ticker, accession, e)
        else:
            logger.debug("index.json returned %s for %s", resp.status_code, index_url)

        # fallback to accession-named .htm
        fallback_url = f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{accession}.htm"
        resp = self._rate_limited_get(fallback_url)
        if resp.status_code == 200:
            return resp.text

        raise RuntimeError(f"Unable to find primary document for {ticker} {accession}")

    def save_filing_html(self, ticker: str, accession: str, html: str):
        dir_path = os.path.join("data", "filings", ticker)
        os.makedirs(dir_path, exist_ok=True)
        path = os.path.join(dir_path, f"{accession}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("Saved filing to %s", path)

    def process_new_filings_for_cik(self, ticker: str, cik: str, checkpoint: Dict,
                                    allowed_forms: set = None, max_per_ticker: int = None) -> List[Dict]:
        try:
            metadata = self.get_filing_metadata_for_cik(cik)
        except HostResolutionError as hre:
            # bubble up so caller can choose to skip this ticker cleanly
            raise hre
        except Exception:
            logger.exception("Failed to fetch metadata for CIK %s (ticker %s). Skipping.", cik, ticker)
            return []

        filings = metadata.get("filings", {}).get("recent", {})
        acc_list = filings.get("accessionNumber", [])
        form_list = filings.get("form", [])
        date_list = filings.get("filingDate", [])
        primary_list = filings.get("primaryDocument", [])

        last_acc = checkpoint.get(ticker)
        new_list = []
        for i, acc in enumerate(acc_list):
            if last_acc is not None and acc <= last_acc:
                continue
            form = form_list[i] if i < len(form_list) else ""
            form_up = (form or "").strip().upper()
            if allowed_forms and form_up not in allowed_forms:
                continue
            new_list.append({
                "ticker": ticker,
                "cik": cik,
                "accession": acc,
                "form": form_up,
                "file_date": date_list[i] if i < len(date_list) else None,
                "primary_document": primary_list[i] if i < len(primary_list) else None
            })

        new_list.sort(key=lambda x: x.get("file_date") or "")
        if max_per_ticker:
            return new_list[-max_per_ticker:]
        return new_list

    def process_filing(self, filing: Dict, checkpoint: Dict, checkpoint_path: str):
        try:
            html = self.download_filing_html(filing["ticker"], filing["cik"], filing["accession"], filing.get("primary_document"))
            self.save_filing_html(filing["ticker"], filing["accession"], html)
            with self.thread_checkpoint_lock:
                checkpoint[filing["ticker"]] = filing["accession"]
                atomic_write_json(checkpoint_path, checkpoint)
            logger.info("Downloaded %s %s", filing["ticker"], filing["accession"])
        except HostResolutionError as hre:
            # unexpected here because _rate_limited_get does host-check early; but handle anyway
            logger.error("DNS resolution failed while downloading filing %s %s: %s", filing["ticker"], filing["accession"], hre)
        except Exception:
            logger.exception("Error while processing filing %s %s", filing.get("ticker"), filing.get("accession"))

    # --- run ingestion ---
    def run(self, tickers: List[str]):
        cik_map = self.map_tickers_to_cik(tickers)
        if not cik_map:
            logger.error("No tickers resolved to CIKs; exiting.")
            return

        checkpoint = load_json(CHECKPOINT_PATH)
        all_filings = []

        # gather filings per ticker; if a ticker's host doesn't resolve we'll skip it
        for ticker, cik in cik_map.items():
            logger.info("Checking filings for %s (CIK %s)", ticker, cik)
            try:
                new_filings = self.process_new_filings_for_cik(ticker, cik, checkpoint, allowed_forms=ALLOWED_FORMS, max_per_ticker=MAX_FILINGS_PER_TICKER)
            except HostResolutionError as hre:
                logger.error("DNS resolution failed for EDGAR host while processing ticker %s: %s. Skipping this ticker.", ticker, hre)
                continue
            all_filings.extend(new_filings)

        logger.info("Total filings to download: %d", len(all_filings))
        futures = [self.executor.submit(self.process_filing, f, checkpoint, CHECKPOINT_PATH) for f in all_filings]

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                # process_filing already logs; make sure executor doesn't crash
                logger.exception("Unhandled exception from worker future")


# ========== Entrypoint ==========
if __name__ == "__main__":
    TICKERS = ["AAPL", "MSFT", "GOOGL"]
    ingestor = EdgarIngestor()
    ingestor.run(TICKERS) 