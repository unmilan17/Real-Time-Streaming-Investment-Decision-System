# file: setup_cik_map.py

#RUN ONLY ONCE TO CREATE CLK MAPPINGS

"""
Robust downloader for SEC company tickers -> CIK mapping.
Saves mapping to data/cik_ticker_map.json as TICKER -> zero-padded CIK (string).
"""
import json
import requests
import os
import time
import logging
from typing import Dict, Any, List

URLS = [
    "https://www.sec.gov/files/company_tickers_exchange.json",
    "https://www.sec.gov/files/company_tickers.json",
    # fallback older path (redirects may exist)
    "https://www.sec.gov/data/company_tickers_exchange.json"
]

USER_AGENT = "MyFirm-InvestPipeline/1.0 (contact@myfirm.com)"
OUT_PATH = "data/cik_ticker_map.json"
os.makedirs("data", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("setup_cik_map")

def fetch_json(url: str, tries: int = 3, backoff: float = 1.0) -> Any:
    headers = {"User-Agent": USER_AGENT}
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("Fetch attempt %d failed for %s: %s", i+1, url, e)
            time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"Failed to fetch JSON from {url} after {tries} attempts")

def try_parse_raw(raw: Any) -> Dict[str, str]:
    """
    Parse raw JSON into mapping {TICKER -> CIKstring}
    Supports:
      - {"fields": [...], "data": [[...], ...]}
      - list of dicts [{"ticker":..., "cik_str":...}, ...]
      - dict-of-dicts { "0": {"ticker":..., "cik_str":...}, ... }
      - fallback heuristics on lists of lists
    """
    mapping = {}

    # Case 1: dict with fields & data
    if isinstance(raw, dict) and "fields" in raw and "data" in raw:
        fields: List[str] = raw.get("fields", [])
        data: List[List[Any]] = raw.get("data", [])
        # Normalize field names to lowercase for matching
        fld_lower = [f.lower() for f in fields]
        # try possible names for ticker and cik
        ticker_keys = ["ticker", "symbol", "spreadticker", "conformed_ticker"]
        cik_keys = ["cik", "cik_str", "cikstr", "cik_number"]
        try:
            ticker_idx = next(i for i, f in enumerate(fld_lower) if f in ticker_keys)
            cik_idx = next(i for i, f in enumerate(fld_lower) if f in cik_keys)
        except StopIteration:
            # fall back to common order guesses: [cik, name, ticker, exchange] or [index, ticker, cik, title]
            # attempt to autodetect indices by inspecting first row
            if len(data) > 0:
                sample = data[0]
                # find element that looks like CIK (all digits)
                cik_idx = next((i for i, v in enumerate(sample) if isinstance(v, int) or (isinstance(v, str) and v.isdigit())), None)
                ticker_idx = next((i for i, v in enumerate(sample) if isinstance(v, str) and v.isupper() and 1 <= len(v) <= 6), None)
            else:
                ticker_idx = cik_idx = None

        if ticker_idx is not None and cik_idx is not None:
            for row in data:
                try:
                    t = str(row[ticker_idx]).upper()
                    c = str(row[cik_idx])
                    # pad to 10 digits
                    c = c.zfill(10)
                    mapping[t] = c
                except Exception:
                    continue
            if mapping:
                return mapping

    # Case 2: list of dicts
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        for entry in raw:
            # look for 'ticker' key (case-insensitive)
            keys = {k.lower(): k for k in entry.keys()}
            if "ticker" in keys:
                t = str(entry[keys["ticker"]]).upper()
                cik_key = next((k for k in ["cik_str", "cik", "cikstr"] if k in keys), None)
                if cik_key:
                    c = str(entry[keys[cik_key]])
                    mapping[t] = c.zfill(10)
        if mapping:
            return mapping

    # Case 3: dict of dicts
    if isinstance(raw, dict):
        # try values()
        for val in raw.values():
            if isinstance(val, dict):
                # attempt to extract
                keys = {k.lower(): k for k in val.keys()}
                if "ticker" in keys and any(k in keys for k in ["cik_str", "cik", "cikstr"]):
                    cik_key = next(k for k in ["cik_str", "cik", "cikstr"] if k in keys)
                    try:
                        t = str(val[keys["ticker"]]).upper()
                        c = str(val[cik_key]).zfill(10)
                        mapping[t] = c
                    except Exception:
                        continue
        if mapping:
            return mapping

    # Final fallback: try heuristics if it's a simple list of lists
    if isinstance(raw, list) and raw and isinstance(raw[0], list):
        for row in raw:
            # pick CIK as element with digits, ticker as UPPER short string
            cik_candidates = [str(x) for x in row if isinstance(x, (int,)) or (isinstance(x, str) and x.isdigit())]
            ticker_candidates = [str(x) for x in row if isinstance(x, str) and x.isalpha() and x.upper()==x and 1 <= len(x) <= 6]
            if cik_candidates and ticker_candidates:
                t = ticker_candidates[0].upper()
                c = cik_candidates[0].zfill(10)
                mapping[t] = c
        if mapping:
            return mapping

    raise RuntimeError("Could not parse SEC ticker->CIK JSON: unsupported schema")

def main():
    last_exc = None
    for url in URLS:
        try:
            logger.info("Attempting download from %s", url)
            raw = fetch_json(url)
            mapping = try_parse_raw(raw)
            # save mapping
            with open(OUT_PATH, "w") as f:
                json.dump(mapping, f, indent=2)
            logger.info("✅ Saved %d ticker->CIK entries to %s", len(mapping), OUT_PATH)
            return
        except Exception as e:
            logger.warning("Failed to parse URL %s: %s", url, e)
            last_exc = e
    # if we reach here, all URLs failed
    raise last_exc

if __name__ == "__main__":
    main()
