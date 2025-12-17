"""
SEC Form 4 Filing Parser using LiteLLM with MongoDB Storage
Extracts structured data from HTML filings and stores in MongoDB
"""

import json
import os
import argparse
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
from litellm import completion
from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
from pymongo.errors import DuplicateKeyError, BulkWriteError


class TransactionDetail(BaseModel):
    """Individual transaction within the filing"""
    security_title: str = Field(description="Title of the security (e.g., Common Stock)")
    transaction_date: str = Field(description="Date of transaction in YYYY-MM-DD format")
    transaction_code: str = Field(description="Transaction code (e.g., S for sale, P for purchase)")
    shares_traded: float = Field(description="Number of shares acquired or disposed")
    acquisition_or_disposition: str = Field(description="Whether shares were acquired (A) or disposed (D)")
    price_per_share: Optional[float] = Field(None, description="Price per share in USD")
    shares_owned_after: float = Field(description="Total shares owned after transaction")
    ownership_type: str = Field(description="Direct (D) or Indirect (I) ownership")
    rule_10b5_1_plan: bool = Field(False, description="Whether transaction was under Rule 10b5-1 plan")

    @field_validator('transaction_date')
    @classmethod
    def validate_date(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except Exception:
            return v


class ReportingPerson(BaseModel):
    """Information about the person filing the report"""
    name: str = Field(description="Full name of reporting person")
    relationship_to_issuer: List[str] = Field(description="Roles: Director, Officer, 10% Owner, Other")
    officer_title: Optional[str] = Field(None, description="Title if reporting person is an officer")
    address: Dict[str, str] = Field(description="Address components: street, city, state, zip")


class IssuerInfo(BaseModel):
    """Information about the issuing company"""
    company_name: str = Field(description="Name of the issuer company")
    ticker_symbol: str = Field(description="Stock ticker symbol")
    cik: Optional[str] = Field(None, description="Central Index Key")


class SECForm4Filing(BaseModel):
    """Complete structured representation of SEC Form 4 filing"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "filing_metadata": {
                    "filing_date": "2025-08-12",
                    "earliest_transaction_date": "2025-08-08",
                    "is_amendment": False,
                    "source_file": "0001767094-25-000005.html"
                },
                "issuer": {
                    "company_name": "Apple Inc.",
                    "ticker_symbol": "AAPL",
                    "cik": "0000320193"
                },
                "reporting_person": {
                    "name": "O'Brien Deirdre",
                    "relationship_to_issuer": ["Officer"],
                    "officer_title": "Senior Vice President",
                    "address": {
                        "street": "One Apple Park Way",
                        "city": "Cupertino",
                        "state": "CA",
                        "zip": "95014"
                    }
                },
                "transactions": [{
                    "security_title": "Common Stock",
                    "transaction_date": "2025-08-08",
                    "transaction_code": "S",
                    "shares_traded": 34821,
                    "acquisition_or_disposition": "D",
                    "price_per_share": 223.20,
                    "shares_owned_after": 136687,
                    "ownership_type": "D",
                    "rule_10b5_1_plan": True
                }],
                "total_transaction_value": 7771726.00,
                "footnotes": ["Transaction executed under Rule 10b5-1 plan"],
                "parsed_timestamp": "2025-11-01T10:30:00Z"
            }
        }
    )

    filing_metadata: Dict[str, Any] = Field(description="Filing date, amendment status, source file, etc.")
    issuer: IssuerInfo
    reporting_person: ReportingPerson
    transactions: List[TransactionDetail] = Field(description="All non-derivative security transactions")
    derivative_transactions: List[Dict[str, Any]] = Field(default_factory=list, description="Derivative security transactions")
    total_transaction_value: Optional[float] = Field(None, description="Estimated total transaction value in USD")
    footnotes: List[str] = Field(default_factory=list, description="Explanatory footnotes from filing")
    parsed_timestamp: str = Field(description="ISO timestamp when parsing occurred")


EXTRACTION_PROMPT_TEMPLATE = """You are a specialized financial data extraction system parsing SEC Form 4 insider trading filings.

EXTRACTION TASK:
Parse the HTML Form 4 filing below and extract ALL information into the specified JSON structure.

CRITICAL PARSING RULES:

1. TABLE I - NON-DERIVATIVE SECURITIES (Common Stock transactions):
   - Look for the HTML table with header "Table I - Non-Derivative Securities"
   - Extract EVERY row in the table body (tbody)
   - For each row extract:
     * Security title (column 1)
     * Transaction date (column 2) - MUST convert MM/DD/YYYY to YYYY-MM-DD format
     * Transaction code (column 3) - S, P, A, M, F, G, etc.
     * Amount/shares (column 4) - MUST be integer (no decimals)
     * (A) or (D) indicator (column 5) 
     * Price (column 6) - remove $ symbol, keep as float, handle missing prices as null
     * Shares owned after (column 7) - MUST be integer (no decimals)
     * Ownership type D or I (column 8)

2. TRANSACTION CODES:
   - S = Sale
   - P = Purchase  
   - A = Award/Grant
   - M = Option Exercise
   - G = Gift
   - F = Payment of exercise price or tax withholding

3. RULE 10b5-1 PLAN:
   - Check if there's an X in the checkbox for "transaction was made pursuant to Rule 10b5-1"
   - OR check footnotes for mention of "10b5-1" or "Rule 10b5-1"
   - Set rule_10b5_1_plan to true for each transaction if found

4. REPORTING PERSON (Section 1):
   - Name from top of section 1
   - Address: street, city, state, zip

5. ISSUER (Section 2):
   - Company name and ticker symbol in square brackets

6. RELATIONSHIP (Section 5):
   - Check which boxes are marked (X):
     * Director
     * Officer (with title below)
     * 10% Owner
     * Other

7. DATES (Section 3):
   - Extract "Date of Earliest Transaction" - MUST convert to YYYY-MM-DD format

8. FOOTNOTES:
   - Extract all numbered footnotes from "Explanation of Responses" section
   - These provide critical context for transactions

9. CALCULATIONS:
   - total_transaction_value is calculated by the system, do NOT include it in your response
   - The system will compute: sum of (shares_traded * price_per_share) for transactions with codes S, P (actual market transactions)
   - Excludes M (option exercise), F (tax withholding), A (awards) from value calculation

10. DERIVATIVE TRANSACTIONS (Table II):
   - Extract derivative securities using CLEAN, SHORT keys:
     * "title" for security title
     * "conversion_price" for conversion/exercise price
     * "transaction_date" for transaction date (YYYY-MM-DD format)
     * "transaction_code" for code
     * "acquired" for number acquired
     * "disposed" for number disposed
     * "date_exercisable" for exercisable date
     * "expiration_date" for expiration date
     * "underlying_title" for underlying security title
     * "underlying_amount" for underlying amount (integer)
     * "price" for derivative price
     * "beneficially_owned_after" for shares owned after (integer)
     * "ownership_form" for D or I
     * "nature_of_indirect" for indirect ownership description

11. DATE FORMAT CONSISTENCY:
   - ALL dates must be in YYYY-MM-DD format (ISO 8601)
   - Convert MM/DD/YYYY to YYYY-MM-DD everywhere

12. NUMERIC TYPE CONSISTENCY:
   - Shares/amounts must be integers (no .0 decimals)
   - Prices remain as floats

JSON SCHEMA:
{schema}

HTML FILING:
{html_content}

RESPONSE FORMAT:
Return ONLY a valid JSON object matching the schema. No markdown, no explanations, just JSON.
Do NOT include total_transaction_value in your response - the system calculates it.

BEGIN EXTRACTION NOW:
"""


class MongoDBHandler:
    """Handles MongoDB connection and operations for SEC filings"""
    
    def __init__(self, connection_string: str = None, database_name: str = "earnings_db"):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection string (defaults to env var MONGODB_URI)
            database_name: Name of the database to use
        """
        self.connection_string = connection_string or os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        self.database_name = database_name
        
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[database_name]
            self.filings_collection = self.db["filings"]
            
            # Test connection
            self.client.server_info()
            print(f"✓ Connected to MongoDB: {database_name}")
            
            # Create indexes
            self._create_indexes()
            
        except Exception as e:
            raise RuntimeError(f"Failed to connect to MongoDB: {e}")
    
    def _create_indexes(self):
        """Create optimized indexes for querying SEC filings"""
        
        indexes = [
            # Unique compound index for document identification (prevents duplicates)
            IndexModel(
                [("metadata.ticker", ASCENDING), 
                 ("metadata.doc_id", ASCENDING)],
                unique=True,
                name="idx_ticker_docid_unique"
            ),
            
            # Query by ticker and date range (most common query pattern)
            IndexModel(
                [("metadata.ticker", ASCENDING),
                 ("metadata.date", DESCENDING)],
                name="idx_ticker_date"
            ),
            
            # Query by reporting person
            IndexModel(
                [("metadata.reporting_person", ASCENDING),
                 ("metadata.date", DESCENDING)],
                name="idx_reporting_person_date"
            ),
            
            # Query by CIK
            IndexModel(
                [("metadata.cik", ASCENDING),
                 ("metadata.date", DESCENDING)],
                name="idx_cik_date"
            ),
            
            # Query by filing date
            IndexModel(
                [("metadata.filing_date", DESCENDING)],
                name="idx_filing_date"
            ),
            
            # Query by transaction value (for finding large transactions)
            IndexModel(
                [("metadata.total_transaction_value", DESCENDING)],
                name="idx_transaction_value"
            ),
            
            # Compound index for ticker + transaction date + value
            IndexModel(
                [("metadata.ticker", ASCENDING),
                 ("metadata.date", DESCENDING),
                 ("metadata.total_transaction_value", DESCENDING)],
                name="idx_ticker_date_value"
            ),
            
            # Text index for searching by company name or reporting person
            IndexModel(
                [("issuer.company_name", "text"),
                 ("reporting_person.name", "text")],
                name="idx_text_search"
            ),
            
            # Index for transaction codes (nested array queries)
            IndexModel(
                [("transactions.transaction_code", ASCENDING)],
                name="idx_transaction_code"
            ),
            
            # Index for parsed timestamp (data freshness queries)
            IndexModel(
                [("parsed_timestamp", DESCENDING)],
                name="idx_parsed_timestamp"
            )
        ]
        
        try:
            self.filings_collection.create_indexes(indexes)
            print("✓ MongoDB indexes created/verified")
        except Exception as e:
            print(f"Warning: Could not create all indexes: {e}")
    
    def insert_filing(self, filing_data: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
        """
        Insert a single filing document into MongoDB
        
        Args:
            filing_data: Parsed filing data
            metadata: Filing metadata
            
        Returns:
            True if successful, False if duplicate
        """
        try:
            # Combine filing data and metadata into single document
            document = {
                **filing_data,
                "metadata": metadata,
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }
            
            self.filings_collection.insert_one(document)
            return True
            
        except DuplicateKeyError:
            print(f"  ⚠ Duplicate filing skipped: {metadata.get('doc_id')}")
            return False
        except Exception as e:
            print(f"  ✗ MongoDB insert error: {e}")
            raise
    
    def get_filing_count(self, ticker: str = None) -> int:
        """Get count of filings, optionally filtered by ticker"""
        query = {"metadata.ticker": ticker} if ticker else {}
        return self.filings_collection.count_documents(query)
    
    def close(self):
        """Close MongoDB connection"""
        if hasattr(self, 'client'):
            self.client.close()
            print("✓ MongoDB connection closed")


class SECFilingParser:
    """Production-grade SEC Form 4 parser using LLM extraction"""

    def __init__(self, model_name: str = "gemini/gemini-2.5-flash-lite", max_retries: int = 3, verbose: bool = False):
        """
        Initialize parser with LiteLLM model

        Args:
            model_name: LiteLLM model identifier for Gemini
            max_retries: Maximum number of retry attempts for API calls
            verbose: Enable verbose logging
        """
        self.model_name = model_name
        self.max_retries = max_retries
        self.verbose = verbose
        self.schema_json = json.dumps(SECForm4Filing.model_json_schema(), indent=2)

        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")

        os.environ["GEMINI_API_KEY"] = api_key

    def _prepare_prompt(self, html_content: str) -> str:
        """Prepare extraction prompt with schema and content"""
        return EXTRACTION_PROMPT_TEMPLATE.format(
            schema=self.schema_json,
            html_content=html_content[:50000]
        )

    def _call_llm_with_retry(self, prompt: str) -> str:
        """Call LLM with retry logic"""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                if self.verbose:
                    print(f"  LLM API call attempt {attempt + 1}...")

                response = completion(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    max_tokens=8192
                )

                if hasattr(response, "choices"):
                    return response.choices[0].message.content
                if isinstance(response, dict) and "choices" in response:
                    return response["choices"][0]["message"]["content"]
                return str(response)

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    print(f"  LLM call attempt {attempt + 1} failed: {e}. Retrying...")
                    continue
                else:
                    raise RuntimeError(f"LLM call failed after {self.max_retries} attempts: {e}")

        raise RuntimeError(f"LLM call failed: {last_error}")

    def _normalize_dates(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure all dates are in YYYY-MM-DD format"""
        def convert_date(date_str: Optional[str]) -> Optional[str]:
            if not date_str:
                return date_str
            try:
                if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
                    return date_str
                if '/' in date_str:
                    dt = datetime.strptime(date_str, '%m/%d/%Y')
                    return dt.strftime('%Y-%m-%d')
                dt = datetime.fromisoformat(date_str)
                return dt.strftime('%Y-%m-%d')
            except Exception:
                return date_str

        if 'filing_metadata' in data:
            if 'filing_date' in data['filing_metadata']:
                data['filing_metadata']['filing_date'] = convert_date(data['filing_metadata']['filing_date'])
            if 'earliest_transaction_date' in data['filing_metadata']:
                data['filing_metadata']['earliest_transaction_date'] = convert_date(data['filing_metadata']['earliest_transaction_date'])

        if 'transactions' in data:
            for txn in data['transactions']:
                if 'transaction_date' in txn:
                    txn['transaction_date'] = convert_date(txn.get('transaction_date'))

        if 'derivative_transactions' in data:
            for dtxn in data['derivative_transactions']:
                if 'transaction_date' in dtxn:
                    dtxn['transaction_date'] = convert_date(dtxn.get('transaction_date'))
                if 'date_exercisable' in dtxn:
                    dtxn['date_exercisable'] = convert_date(dtxn.get('date_exercisable'))
                if 'expiration_date' in dtxn:
                    dtxn['expiration_date'] = convert_date(dtxn.get('expiration_date'))

        return data

    def _normalize_numeric_types(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure shares/amounts are integers and prices are floats"""
        if 'transactions' in data:
            for txn in data['transactions']:
                if 'shares_traded' in txn and txn['shares_traded'] is not None:
                    try:
                        txn['shares_traded'] = int(float(txn['shares_traded']))
                    except Exception:
                        pass
                if 'shares_owned_after' in txn and txn['shares_owned_after'] is not None:
                    try:
                        txn['shares_owned_after'] = int(float(txn['shares_owned_after']))
                    except Exception:
                        pass
                if 'price_per_share' in txn and txn['price_per_share'] is not None:
                    try:
                        txn['price_per_share'] = float(txn['price_per_share'])
                    except Exception:
                        txn['price_per_share'] = None

        if 'derivative_transactions' in data:
            for dtxn in data['derivative_transactions']:
                if 'acquired' not in dtxn or dtxn.get('acquired') is None:
                    dtxn['acquired'] = 0
                else:
                    try:
                        dtxn['acquired'] = int(float(dtxn['acquired']))
                    except Exception:
                        dtxn['acquired'] = 0

                for field in ['disposed', 'underlying_amount', 'beneficially_owned_after']:
                    if field in dtxn and dtxn[field] is not None:
                        try:
                            dtxn[field] = int(float(dtxn[field]))
                        except Exception:
                            pass

                for field in ['conversion_price', 'price']:
                    if field in dtxn and dtxn[field] is not None:
                        try:
                            dtxn[field] = float(dtxn[field])
                        except Exception:
                            pass

        return data

    def _calculate_transaction_value(self, data: Dict[str, Any]) -> float:
        """Calculate total transaction value from market transactions only"""
        total = 0.0
        market_codes = {'S', 'P'}

        transactions = data.get('transactions', [])
        for txn in transactions:
            code = str(txn.get('transaction_code', '')).upper()
            if code in market_codes:
                shares = txn.get('shares_traded')
                price = txn.get('price_per_share')

                if shares is not None and price is not None:
                    try:
                        total += float(shares) * float(price)
                    except (ValueError, TypeError):
                        continue

        return round(total, 2)

    def _strip_code_fences_and_extract_json(self, text: str) -> str:
        """Remove code fences and extract JSON"""
        if not text:
            return text

        text = text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I)
        text = re.sub(r"\s*```$", "", text, flags=re.I)

        if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
            return text

        first_brace = text.find("{")
        if first_brace == -1:
            return text

        stack = []
        for i in range(first_brace, len(text)):
            ch = text[i]
            if ch == "{":
                stack.append("{")
            elif ch == "}":
                stack.pop()
                if not stack:
                    return text[first_brace:i + 1]

        return text

    def _validate_and_clean_json(self, llm_response: str) -> Dict[str, Any]:
        """Validate LLM response and ensure it matches schema"""
        try:
            if self.verbose:
                print(f"  Raw LLM response length: {len(llm_response) if llm_response else 0}")

            cleaned = self._strip_code_fences_and_extract_json(llm_response)
            parsed_data = json.loads(cleaned)

            if self.verbose:
                print("  Parsed JSON from LLM. Validating against Pydantic schema...")

            validated = SECForm4Filing(**parsed_data)
            data = validated.model_dump()

            data = self._normalize_dates(data)
            data = self._normalize_numeric_types(data)

            if 'derivative_transactions' in data:
                for dtxn in data['derivative_transactions']:
                    if 'acquired' not in dtxn or dtxn.get('acquired') is None:
                        dtxn['acquired'] = 0

            footnotes = data.get('footnotes', []) or []
            has_10b5 = any('10b5' in str(fn).lower() for fn in footnotes)

            for txn in data.get('transactions', []):
                code = str(txn.get('transaction_code', '')).upper()
                if has_10b5:
                    txn['rule_10b5_1_plan'] = (code in {'S', 'P'})
                else:
                    txn.setdefault('rule_10b5_1_plan', False)

            data['total_calculation_method'] = "sum of shares_traded * price_per_share for transactions with code in {S, P}"
            data['total_transaction_value'] = self._calculate_transaction_value(data)

            if not data.get('parsed_timestamp'):
                data['parsed_timestamp'] = datetime.now(timezone.utc).isoformat()

            return data

        except json.JSONDecodeError as e:
            if self.verbose:
                print("  ERROR: Invalid JSON from LLM. Attempting recovery...")
            candidate = self._strip_code_fences_and_extract_json(llm_response)
            try:
                parsed_data = json.loads(candidate)
                validated = SECForm4Filing(**parsed_data)
                data = validated.model_dump()
                data = self._normalize_dates(data)
                data = self._normalize_numeric_types(data)
                
                footnotes = data.get('footnotes', []) or []
                has_10b5 = any('10b5' in str(fn).lower() for fn in footnotes)
                for txn in data.get('transactions', []):
                    code = str(txn.get('transaction_code', '')).upper()
                    if has_10b5:
                        txn['rule_10b5_1_plan'] = (code in {'S', 'P'})
                    else:
                        txn.setdefault('rule_10b5_1_plan', False)
                
                data['total_calculation_method'] = "sum of shares_traded * price_per_share for transactions with code in {S, P}"
                data['total_transaction_value'] = self._calculate_transaction_value(data)
                if not data.get('parsed_timestamp'):
                    data['parsed_timestamp'] = datetime.now(timezone.utc).isoformat()
                return data
            except Exception:
                raise ValueError(f"LLM response is not valid JSON: {e}")
        except Exception as e:
            if self.verbose:
                print(f"  Schema validation / cleaning error: {e}")
            raise ValueError(f"Failed to validate and clean LLM output: {e}")

    def _extract_metadata_from_html(self, html_content: str, source_filename: str, ticker: str,
                                    parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract filing metadata"""
        doc_id = source_filename.replace('.html', '')

        filing_date = parsed_data.get('filing_metadata', {}).get('filing_date', '')
        transaction_date = parsed_data.get('filing_metadata', {}).get('earliest_transaction_date', '')
        is_amendment = parsed_data.get('filing_metadata', {}).get('is_amendment', False)

        cik = parsed_data.get('issuer', {}).get('cik', '')
        reporting_person = parsed_data.get('reporting_person', {}).get('name', 'Unknown')

        total_value = parsed_data.get('total_transaction_value', 0) or 0
        num_transactions = len(parsed_data.get('transactions', []))

        metadata = {
            "ticker": ticker.upper(),
            "cik": cik,
            "doc_type": "filing",
            "doc_id": doc_id,
            "date": transaction_date,
            "filing_date": filing_date,
            "source_file": source_filename,
            "reporting_person": reporting_person.upper(),
            "is_amendment": is_amendment,
            "total_transaction_value": round(total_value, 2),
            "num_transactions": num_transactions
        }

        return metadata

    def parse_filing(self, html_content: str, source_filename: str = "", ticker: str = "") -> Dict[str, Any]:
        """Parse HTML filing and extract structured data"""
        prompt = self._prepare_prompt(html_content)

        try:
            llm_output = self._call_llm_with_retry(prompt)
            validated_data = self._validate_and_clean_json(llm_output)
            validated_data['parsed_timestamp'] = datetime.now(timezone.utc).isoformat()

            if source_filename and 'filing_metadata' in validated_data:
                validated_data['filing_metadata']['source_file'] = source_filename

            return validated_data

        except Exception as e:
            raise RuntimeError(f"Filing parsing failed: {str(e)}")


def process_ticker_filings(data_dir: Path, ticker: str, mongodb_handler: MongoDBHandler, verbose: bool = False):
    """
    Process all HTML filings for a specific ticker and store in MongoDB
    
    Args:
        data_dir: Base data directory containing filings folder
        ticker: Stock ticker symbol
        mongodb_handler: MongoDB handler instance
        verbose: Enable verbose logging
    """
    filings_dir = data_dir / "filings" / ticker

    if not filings_dir.exists():
        print(f"Error: Filings directory not found: {filings_dir}")
        return

    html_files = sorted(list(filings_dir.glob("*.html")))

    if not html_files:
        print(f"No HTML files found in {filings_dir}")
        return

    print(f"\n{'=' * 70}")
    print(f"Processing {len(html_files)} HTML files for {ticker}")
    print(f"Target: MongoDB database '{mongodb_handler.database_name}'")
    print(f"{'=' * 70}\n")

    parser = SECFilingParser(verbose=verbose)

    successful = 0
    failed = 0
    duplicates = 0

    for idx, html_file in enumerate(html_files, 1):
        print(f"[{idx}/{len(html_files)}] Processing: {html_file.name}")

        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()

            result = parser.parse_filing(html_content, html_file.name, ticker)

            metadata = parser._extract_metadata_from_html(
                html_content,
                html_file.name,
                ticker,
                result
            )

            # Insert into MongoDB
            insert_success = mongodb_handler.insert_filing(result, metadata)
            
            if insert_success:
                num_transactions = len(result.get('transactions', []))
                num_derivatives = len(result.get('derivative_transactions', []))
                total_value = result.get('total_transaction_value', 0)

                print(f"  ✓ Stored in MongoDB")
                print(f"  Reporting Person: {result['reporting_person']['name']}")
                print(f"  Non-Derivative Transactions: {num_transactions}")
                print(f"  Derivative Transactions: {num_derivatives}")

                if num_transactions > 0 and total_value:
                    print(f"  Total Transaction Value: ${total_value:,.2f}")
                    for txn in result['transactions'][:3]:
                        price_str = f"${txn['price_per_share']}" if txn.get('price_per_share') else "N/A"
                        shares = txn.get('shares_traded', 0)
                        code = str(txn.get('transaction_code', '')).upper()
                        
                        if code in {'S', 'P'} and txn.get('price_per_share'):
                            value = shares * txn['price_per_share']
                            print(f"    - {code}: {shares:,} shares @ {price_str} = ${value:,.2f}")
                        else:
                            print(f"    - {code}: {shares:,} shares @ {price_str}")

                    if num_transactions > 3:
                        print(f"    ... and {num_transactions - 3} more transactions")

                successful += 1
            else:
                duplicates += 1

        except Exception as e:
            print(f"  ✗ FAILED: {str(e)}")
            if verbose:
                import traceback
                traceback.print_exc()
            failed += 1

    print(f"\n{'=' * 70}")
    print(f"PROCESSING COMPLETE FOR {ticker}")
    print(f"{'=' * 70}")
    print(f"✓ Successfully stored: {successful}/{len(html_files)}")
    print(f"⚠ Duplicates skipped: {duplicates}/{len(html_files)}")
    print(f"✗ Failed: {failed}/{len(html_files)}")
    print(f"📊 Total filings in DB for {ticker}: {mongodb_handler.get_filing_count(ticker)}")
    print(f"{'=' * 70}\n")


def main():
    """Main entry point for CLI usage"""
    parser = argparse.ArgumentParser(
        description="Extract structured data from SEC Form 4 HTML filings and store in MongoDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using default MongoDB (localhost:27017)
  python llm_extractor_mongodb.py --data-dir data --ticker AAPL
  
  # Using MongoDB Atlas or custom URI
  export MONGODB_URI="mongodb+srv://user:pass@cluster.mongodb.net/"
  python llm_extractor_mongodb.py --data-dir data --ticker GOOGL --verbose
  
  # Custom database name
  python llm_extractor_mongodb.py --data-dir data --ticker MSFT --db-name my_filings

Environment Variables:
  MONGODB_URI: MongoDB connection string (default: mongodb://localhost:27017/)
  GEMINI_API_KEY: Required - Gemini API key for LLM processing

MongoDB Collections:
  - filings: Main collection storing parsed filing data with metadata
  
Indexes Created:
  - Unique: ticker + doc_id (prevents duplicates)
  - Compound: ticker + date (for date range queries)
  - Single: reporting_person, cik, filing_date, transaction_value
  - Text: company_name, reporting_person (for search)
        """
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Base data directory containing filings folder"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        required=True,
        help="Stock ticker symbol (e.g., AAPL, GOOGL, MSFT)"
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default="earnings_db",
        help="MongoDB database name (default: earnings_db)"
    )
    parser.add_argument(
        "--mongodb-uri",
        type=str,
        default=None,
        help="MongoDB connection URI (default: from MONGODB_URI env var or localhost)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for debugging"
    )

    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    ticker = args.ticker.upper()

    try:
        # Initialize MongoDB handler
        mongodb_handler = MongoDBHandler(
            connection_string=args.mongodb_uri,
            database_name=args.db_name
        )

        # Process filings
        process_ticker_filings(data_dir, ticker, mongodb_handler, args.verbose)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        exit(1)
    finally:
        if 'mongodb_handler' in locals():
            mongodb_handler.close()


if __name__ == "__main__":
    main()