import os
import time
import json
import logging
import requests
import threading
import random
import socket
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv
import os

load_dotenv()


ALLOWED_FORMS = {"4"}

MAX_FILINGS_PER_TICKER = 5
REQUESTS_PER_SECOND = 5
RETRY_BACKOFF = 2
MAX_WORKERS = 8
MANUAL_MAX_ATTEMPTS = 5

EDGAR_BASE_URL = os.getenv("EDGAR_BASE_URL", "https://data.sec.gov/submissions")
CIK_TICKER_MAP_PATH = "data/cik_ticker_map.json"
CHECKPOINT_PATH = "data/last_processed.json"

USER_AGENT = os.getenv(
    "EDGAR_USER_AGENT"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("edgar_ingest_form4_only")


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
    pass


class EdgarIngestor:
    def __init__(self, rate_limit: int = REQUESTS_PER_SECOND, user_agent: str = USER_AGENT, max_workers: int = MAX_WORKERS):
        self.rate_limit = rate_limit
        self.user_agent = user_agent
        self.lock = threading.Lock()
        self.last_request_time = 0.0
        self.thread_checkpoint_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

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

    def _host_resolves(self, url: str) -> bool:
        try:
            host = url.split("://", 1)[1].split("/", 1)[0]
            host = host.split(":", 1)[0]
            socket.gethostbyname(host)
            return True
        except Exception as e:
            logger.debug("Host resolution check failed for host extracted from %s: %s", url, e)
            return False

    def _rate_limited_get(self, url: str, timeout: int = 30) -> requests.Response:
        if not self._host_resolves(url):
            host = url.split("://", 1)[1].split("/", 1)[0].split(":", 1)[0]
            msg = f"Host '{host}' could not be resolved (DNS). Will not attempt HTTP request."
            logger.error(msg)
            raise HostResolutionError(msg)

        with self.lock:
            min_interval = 1.0 / float(max(self.rate_limit, 1))
            since = time.time() - self.last_request_time
            if since < min_interval:
                time.sleep(min_interval - since)
            self.last_request_time = time.time()

        backoff = RETRY_BACKOFF
        last_exc = None
        for attempt in range(1, MANUAL_MAX_ATTEMPTS + 1):
            try:
                headers = {"User-Agent": self.user_agent}
                resp = self.session.get(url, headers=headers, timeout=timeout)
            except requests.RequestException as e:
                last_exc = e
                msg = str(e)
                logger.warning("Request exception for %s: %s (attempt %d/%d)", url, e, attempt, MANUAL_MAX_ATTEMPTS)

                if "NameResolutionError" in msg or "Name or service not known" in msg or isinstance(e, requests.exceptions.ConnectionError) and "Temporary failure in name resolution" in msg:
                    logger.error("Detected DNS resolution error for %s: %s", url, e)
                    raise HostResolutionError(f"DNS resolution failure for host in {url}: {e}")

                sleep_time = backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                time.sleep(sleep_time)
                continue

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return resp
            if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                logger.warning("Received status %s for %s (attempt %d/%d)", resp.status_code, url, attempt, MANUAL_MAX_ATTEMPTS)
                sleep_time = backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                time.sleep(sleep_time)
                continue

            resp.raise_for_status()

        if last_exc:
            raise last_exc
        raise RuntimeError(f"Failed to GET {url} after {MANUAL_MAX_ATTEMPTS} attempts")

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
        try:
            cik_int = int(cik)
        except Exception:
            raise ValueError(f"Invalid CIK value: {cik}")

        url = f"{EDGAR_BASE_URL}/CIK{cik_int:010d}.json"
        resp = self._rate_limited_get(url)
        if resp.status_code == 404:
            logger.warning("No submissions JSON found for CIK %s (URL %s returned 404)", cik, url)
            return {}
        return resp.json()

    def _normalize_accession(self, accession: str) -> str:
        return accession.replace("-", "")

    def _is_valid_form4_html(self, html_content: str) -> bool:
        """
        Validate that HTML content is a proper SEC Form 4 filing.
        
        Returns True only if:
        1. Contains "FORM 4" title
        2. Contains "STATEMENT OF CHANGES IN BENEFICIAL OWNERSHIP"
        3. Contains Table I or Table II structure
        4. Does NOT contain proxy/voting materials keywords
        """
        html_lower = html_content.lower()
        
        excluded_keywords = [
            "schedule 14a",
            "proxy statement",
            "defa14a",
            "def 14a",
            "voting instruction",
            "annual meeting",
            "shareholder meeting",
            "form 8-k",
            "current report",
            "inline xbrl",
            "xbrli:context"
        ]
        
        for keyword in excluded_keywords:
            if keyword in html_lower:
                logger.debug("HTML rejected: contains excluded keyword '%s'", keyword)
                return False
        
        required_form4_markers = [
            "form 4",
            "statement of changes in beneficial ownership"
        ]
        
        for marker in required_form4_markers:
            if marker not in html_lower:
                logger.debug("HTML rejected: missing required marker '%s'", marker)
                return False
        
        table_markers = [
            "table i - non-derivative securities",
            "table ii - derivative securities"
        ]
        
        has_table = any(marker in html_lower for marker in table_markers)
        if not has_table:
            logger.debug("HTML rejected: no Form 4 transaction tables found")
            return False
        
        form4_structure_patterns = [
            r'<td[^>]*class=["\']FormName["\'][^>]*>FORM\s*4</td>',
            r'name and address of reporting person',
            r'issuer name.*ticker.*trading symbol',
            r'relationship of reporting person',
            r'transaction code'
        ]
        
        structure_matches = sum(1 for pattern in form4_structure_patterns if re.search(pattern, html_lower))
        if structure_matches < 3:
            logger.debug("HTML rejected: insufficient Form 4 structural elements (%d/5)", structure_matches)
            return False
        
        logger.debug("HTML validated as proper Form 4")
        return True

    def download_filing_html(self, ticker: str, cik: str, accession: str, primary_document: Optional[str] = None) -> Optional[str]:
        """
        Download and validate Form 4 HTML filing.
        Returns HTML content only if it passes Form 4 validation.
        """
        cik_nozeros = str(int(cik))
        acc_nodash = self._normalize_accession(accession)

        candidates_to_try = []
        
        if primary_document:
            candidates_to_try.append(f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{primary_document}")
        
        candidates_to_try.append(f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{accession}.htm")
        candidates_to_try.append(f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{accession}.html")
        
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/index.json"
        try:
            resp = self._rate_limited_get(index_url)
            if resp.status_code == 200:
                listing = resp.json()
                items = listing.get("directory", {}).get("item", [])
                
                html_files = [
                    it.get("name") for it in items 
                    if isinstance(it.get("name"), str) and it.get("name", "").lower().endswith((".htm", ".html"))
                ]
                
                for name in html_files:
                    file_url = f"https://www.sec.gov/Archives/edgar/data/{cik_nozeros}/{acc_nodash}/{name}"
                    if file_url not in candidates_to_try:
                        candidates_to_try.append(file_url)
        except Exception as e:
            logger.debug("Could not fetch index.json for %s: %s", accession, e)

        for url in candidates_to_try:
            try:
                logger.debug("Trying URL: %s", url)
                resp = self._rate_limited_get(url)
                
                if resp.status_code != 200:
                    logger.debug("URL %s returned status %d", url, resp.status_code)
                    continue
                
                html_content = resp.text
                
                if self._is_valid_form4_html(html_content):
                    logger.info("Valid Form 4 HTML found at %s", url)
                    return html_content
                else:
                    logger.warning("HTML at %s does not match Form 4 format (may be proxy/8-K/other)", url)
                    
            except Exception as e:
                logger.debug("Error fetching %s: %s", url, e)
                continue

        logger.error("No valid Form 4 HTML found for %s accession %s after trying %d URLs", 
                    ticker, accession, len(candidates_to_try))
        return None

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
            
            if form_up != "4":
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
            html = self.download_filing_html(
                filing["ticker"], 
                filing["cik"], 
                filing["accession"], 
                filing.get("primary_document")
            )
            
            if html is None:
                logger.warning(
                    "Skipping %s %s: Could not retrieve valid Form 4 HTML", 
                    filing["ticker"], 
                    filing["accession"]
                )
                return
                
            self.save_filing_html(filing["ticker"], filing["accession"], html)
            
            with self.thread_checkpoint_lock:
                checkpoint[filing["ticker"]] = filing["accession"]
                atomic_write_json(checkpoint_path, checkpoint)
                
            logger.info("Downloaded Form 4: %s %s", filing["ticker"], filing["accession"])
            
        except HostResolutionError as hre:
            logger.error("DNS resolution failed while downloading filing %s %s: %s", 
                        filing["ticker"], filing["accession"], hre)
        except Exception:
            logger.exception("Error while processing filing %s %s", 
                           filing.get("ticker"), filing.get("accession"))

    def run(self, tickers: List[str]):
        cik_map = self.map_tickers_to_cik(tickers)
        if not cik_map:
            logger.error("No tickers resolved to CIKs; exiting.")
            return

        checkpoint = load_json(CHECKPOINT_PATH)
        all_filings = []

        for ticker, cik in cik_map.items():
            logger.info("Checking Form 4 filings for %s (CIK %s)", ticker, cik)
            try:
                new_filings = self.process_new_filings_for_cik(
                    ticker, cik, checkpoint, 
                    allowed_forms=ALLOWED_FORMS, 
                    max_per_ticker=MAX_FILINGS_PER_TICKER
                )
            except HostResolutionError as hre:
                logger.error("DNS resolution failed for EDGAR host while processing ticker %s: %s. Skipping this ticker.", ticker, hre)
                continue
            all_filings.extend(new_filings)

        logger.info("Total Form 4 filings to download: %d", len(all_filings))
        
        futures = [
            self.executor.submit(self.process_filing, f, checkpoint, CHECKPOINT_PATH) 
            for f in all_filings
        ]

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                logger.exception("Unhandled exception from worker future")


if __name__ == "__main__":
    TICKERS = ["AAPL", "MSFT", "GOOGL"]
    ingestor = EdgarIngestor()
    ingestor.run(TICKERS)