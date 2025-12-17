import os
import logging
from pathlib import Path
from typing import Optional, List
import html2text
from bs4 import BeautifulSoup

# ===============================
# CONFIGURATION
# ===============================
DATA_ROOT = Path(__file__).resolve().parent / "data"
FILINGS_DIR = DATA_ROOT / "filings"
OUTPUT_DIR = DATA_ROOT / "parsed_txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("filing_parser")

# ===============================
# CLASS: FilingParser
# ===============================
class FilingParser:
    """
    Converts SEC filing HTML files (from each ticker directory) into cleaned text.
    """

    def __init__(self):
        self.converter = html2text.HTML2Text()
        self.converter.ignore_links = True
        self.converter.ignore_images = True
        self.converter.body_width = 0  # Don't wrap lines

    def get_clean_text_chunk(self, soup_element) -> Optional[str]:
        """Convert a BeautifulSoup element (like a table) to clean text."""
        if not soup_element:
            return None
        html_string = str(soup_element)
        text_chunk = self.converter.handle(html_string)
        return '\n'.join(line for line in text_chunk.splitlines() if line.strip())

    def parse_file(self, html_file: Path) -> Optional[str]:
        """
        Parse a single HTML filing file and return cleaned text content.
        """
        try:
            with open(html_file, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f.read(), "html.parser")
        except Exception as e:
            logger.error(f"Error reading {html_file}: {e}")
            return None

        all_text_chunks: List[str] = []

        # --- 1. Filing Metadata ---
        info_header = soup.find(string=lambda t: t and "Name and Address of Reporting Person" in t)
        if info_header:
            info_table = info_header.find_parent('table')
            info_chunk = self.get_clean_text_chunk(info_table)
            if info_chunk:
                all_text_chunks.append("--- FILING METADATA ---\n" + info_chunk)

        # --- 2. Table I ---
        table_i_header_tag = soup.find('b', string=lambda t: t and t.strip().startswith('Table I'))
        if table_i_header_tag:
            table_i = table_i_header_tag.find_parent('table')
            table_i_chunk = self.get_clean_text_chunk(table_i)
            if table_i_chunk:
                all_text_chunks.append(table_i_chunk)

        # --- 3. Table II ---
        table_ii_header_tag = soup.find('b', string=lambda t: t and t.strip().startswith('Table II'))
        if table_ii_header_tag:
            table_ii = table_ii_header_tag.find_parent('table')
            table_ii_chunk = self.get_clean_text_chunk(table_ii)
            if table_ii_chunk:
                all_text_chunks.append(table_ii_chunk)

        # --- 4. Explanation of Responses ---
        explanation_header = soup.find('b', string=lambda t: t and t.strip() == 'Explanation of Responses:')
        if explanation_header:
            explanation_table = explanation_header.find_parent('table')
            explanation_chunk = self.get_clean_text_chunk(explanation_table)
            if explanation_chunk:
                all_text_chunks.append(explanation_chunk)

        # --- 5. Signature ---
        sig_header = soup.find(string=lambda t: t and "** Signature of Reporting Person" in t)
        if sig_header:
            sig_table = sig_header.find_parent('table')
            sig_chunk = self.get_clean_text_chunk(sig_table)
            if sig_chunk:
                all_text_chunks.append("--- SIGNATURES ---\n" + sig_chunk)

        if not all_text_chunks:
            logger.warning(f"No parsable content found in {html_file}")
            return None

        return "\n\n".join(all_text_chunks)

    def process_ticker_folder(self, ticker_folder: Path):
        """
        Process all HTML filings for a given ticker.
        """
        ticker = ticker_folder.name
        output_ticker_dir = OUTPUT_DIR / ticker
        output_ticker_dir.mkdir(parents=True, exist_ok=True)

        html_files = list(ticker_folder.glob("*.html"))
        logger.info(f"Processing {len(html_files)} filings for {ticker}")

        for html_file in html_files:
            output_file = output_ticker_dir / (html_file.stem + ".txt")
            if output_file.exists():
                logger.debug(f"Skipping already processed: {html_file.name}")
                continue

            text_content = self.parse_file(html_file)
            if text_content:
                try:
                    with open(output_file, "w", encoding="utf-8") as f:
                        f.write(text_content)
                    logger.info(f"✅ Saved parsed text for {ticker}/{html_file.name}")
                except Exception as e:
                    logger.error(f"Failed to write {output_file}: {e}")

    def process_all_tickers(self):
        """
        Walk through all ticker subdirectories under `data/filings/`.
        """
        tickers = [p for p in FILINGS_DIR.iterdir() if p.is_dir()]
        logger.info(f"Found {len(tickers)} ticker folders to process.")
        for ticker_folder in tickers:
            self.process_ticker_folder(ticker_folder)


# ===============================
# MAIN EXECUTION
# ===============================
if __name__ == "__main__":
    parser = FilingParser()
    parser.process_all_tickers()
