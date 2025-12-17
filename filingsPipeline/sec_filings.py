"""
SEC 10-Q XBRL Data Pipeline to MongoDB
Fetches quarterly filings, parses XBRL directly with lxml, stores in MongoDB.
Compatible with Python 3.12+
"""

import os
import time
import json
import re
from datetime import datetime, timedelta, UTC
from typing import Dict, List, Optional, Any, Tuple
from functools import wraps
from decimal import Decimal
import requests
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from lxml import etree
from urllib.parse import urljoin
from dotenv import load_dotenv

from logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)

load_dotenv() 
user_name = os.getenv('SEC_USER_NAME')
user_email = os.getenv('SEC_USER_EMAIL')

# Raise an error if the variables aren't set
if not user_name or not user_email:
    raise ValueError("SEC_USER_NAME or SEC_USER_EMAIL not set in .env file")


# Configuration
CONFIG = {
    'USER_AGENT': f"{user_name} {user_email}",
    'SEC_BASE_URL': 'https://data.sec.gov',
    'SEC_EDGAR_URL': 'https://www.sec.gov/cgi-bin/browse-edgar',
    'MONGODB_URI': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'), 
    'DATABASE_NAME': os.getenv('DATABASE_NAME', 'sec_filings'), 
    'REQUEST_DELAY': 0.1,
    'MAX_RETRIES': 5,
    'INITIAL_BACKOFF': 1,
}

# Sample 50 US tickers
TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'V', 'JNJ',
    'WMT', 'JPM', 'MA', 'PG', 'UNH', 'HD', 'CVX', 'LLY', 'ABBV', 'MRK',
    'PEP', 'KO', 'COST', 'AVGO', 'ADBE', 'TMO', 'MCD', 'CSCO', 'ACN', 'ABT',
    'NKE', 'DHR', 'TXN', 'NEE', 'CMCSA', 'WFC', 'CRM', 'VZ', 'DIS', 'INTC',
    'AMD', 'ORCL', 'PM', 'PFE', 'NFLX', 'INTU', 'UNP', 'BMY', 'HON', 'BA'
]

# Common XBRL namespaces
NAMESPACES = {
    'xbrli': 'http://www.xbrl.org/2003/instance',
    'xbrldi': 'http://xbrl.org/2006/xbrldi',
    'xlink': 'http://www.w3.org/1999/xlink',
    'link': 'http://www.xbrl.org/2003/linkbase',
    'us-gaap': 'http://fasb.org/us-gaap/2023',
    'dei': 'http://xbrl.sec.gov/dei/2023',
    'iso4217': 'http://www.xbrl.org/2003/iso4217',
    'ix': 'http://www.xbrl.org/2013/inlineXBRL',
    'ixt': 'http://www.xbrl.org/inlineXBRL/transformation/2015-02-26',
}


def exponential_backoff(max_retries=5, initial_delay=1):
    """Decorator for exponential backoff on failed requests."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError) as e:
                    if attempt == max_retries - 1:
                        logger.error("max_retries_reached", function=func.__name__, error=str(e))
                        raise
                    
                    wait_time = delay * (2 ** attempt)
                    logger.warning("request_retry", attempt=attempt + 1, wait_seconds=wait_time, error=str(e))
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


class SECEdgarClient:
    """Client for fetching data from SEC EDGAR."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': CONFIG['USER_AGENT']})
        self.last_request_time = 0
        self._cik_cache = {}
        self.logger = get_logger(f"{__name__}.SECEdgarClient")
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < CONFIG['REQUEST_DELAY']:
            time.sleep(CONFIG['REQUEST_DELAY'] - elapsed)
        self.last_request_time = time.time()
    
    @exponential_backoff(max_retries=CONFIG['MAX_RETRIES'], initial_delay=CONFIG['INITIAL_BACKOFF'])
    def _get_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """Make a GET request with rate limiting."""
        self._rate_limit()
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response
    
    def get_cik_for_ticker(self, ticker: str) -> Optional[str]:
        """Get CIK code for a given ticker symbol."""
        if ticker in self._cik_cache:
            return self._cik_cache[ticker]
        
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            response = self._get_request(url)
            data = response.json()
            
            for entry in data.values():
                if entry['ticker'].upper() == ticker.upper():
                    cik = str(entry['cik_str']).zfill(10)
                    self._cik_cache[ticker] = cik
                    self.logger.info("cik_found", ticker=ticker, cik=cik)
                    return cik
            
            self.logger.warning("cik_not_found", ticker=ticker)
            return None
        except Exception as e:
            self.logger.error("cik_fetch_error", ticker=ticker, error=str(e), exc_info=True)
            return None
    
    def get_latest_10q_filing(self, cik: str, ticker: str) -> Optional[Dict]:
        """Fetch the latest 10-Q filing metadata for a CIK."""
        try:
            url = f"{CONFIG['SEC_BASE_URL']}/submissions/CIK{cik}.json"
            response = self._get_request(url)
            data = response.json()
            
            filings = data.get('filings', {}).get('recent', {})
            forms = filings.get('form', [])
            accession_numbers = filings.get('accessionNumber', [])
            filing_dates = filings.get('filingDate', [])
            primary_documents = filings.get('primaryDocument', [])
            
            for i, form in enumerate(forms):
                if form == '10-Q':
                    accession = accession_numbers[i].replace('-', '')
                    filing_date = filing_dates[i]
                    primary_doc = primary_documents[i]
                    
                    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}"
                    doc_url = f"{base_url}/{primary_doc}"
                    
                    return {
                        'cik': cik,
                        'ticker': ticker,
                        'accessionNumber': accession_numbers[i],
                        'formType': '10-Q',
                        'filingDate': filing_date,
                        'primaryDocument': primary_doc,
                        'documentUrl': doc_url,
                        'baseUrl': base_url
                    }
            
            self.logger.warning("no_10q_found", cik=cik, ticker=ticker)
            return None
        except Exception as e:
            self.logger.error("10q_fetch_error", cik=cik, ticker=ticker, error=str(e), exc_info=True)
            return None
    
    def download_xbrl_package(self, filing: Dict) -> Optional[Dict[str, str]]:
        """Download XBRL instance document and return paths."""
        try:
            cik = filing['cik']
            accession = filing['accessionNumber'].replace('-', '')
            
            cache_dir = f"./cache/{cik}/{accession}"
            os.makedirs(cache_dir, exist_ok=True)
            
            doc_path = os.path.join(cache_dir, filing['primaryDocument'])
            
            if not os.path.exists(doc_path):
                response = self._get_request(filing['documentUrl'])
                with open(doc_path, 'wb') as f:
                    f.write(response.content)
                self.logger.info("xbrl_downloaded", path=doc_path)
            else:
                self.logger.info("xbrl_cached", path=doc_path)
            
            return {
                'instance': doc_path,
                'base_url': filing['baseUrl']
            }
        except Exception as e:
            self.logger.error("xbrl_download_error", error=str(e), exc_info=True)
            return None


class XBRLParser:
    """Direct XBRL parser using lxml (Python 3.12 compatible)."""
    
    def __init__(self):
        self.namespaces = NAMESPACES.copy()
        self.logger = get_logger(f"{__name__}.XBRLParser")
    
    def parse_filing(self, xbrl_paths: Dict[str, str]) -> Optional[Dict]:
        """Parse XBRL filing and extract structured data."""
        try:
            instance_path = xbrl_paths['instance']
            
            tree = etree.parse(instance_path)
            root = tree.getroot()
            
            self._update_namespaces(root)
            
            contexts = self._extract_contexts(root)
            units = self._extract_units(root)
            facts = self._extract_facts(root, contexts, units)
            footnotes = self._extract_footnotes(root)
            
            return {
                'facts': facts,
                'contexts': contexts,
                'units': units,
                'footnotes': footnotes
            }
        except Exception as e:
            self.logger.error("xbrl_parse_error", error=str(e), exc_info=True)
            return None
    
    def _update_namespaces(self, root):
        """Extract and update namespaces from document."""
        for prefix, uri in root.nsmap.items():
            if prefix and prefix not in self.namespaces:
                self.namespaces[prefix] = uri
    
    def _extract_contexts(self, root) -> List[Dict]:
        """Extract all contexts from XBRL instance."""
        contexts = []
        
        for context_elem in root.findall('.//xbrli:context', self.namespaces):
            try:
                context_id = context_elem.get('id')
                
                entity = context_elem.find('.//xbrli:entity/xbrli:identifier', self.namespaces)
                entity_id = entity.text if entity is not None else None
                
                period = {}
                instant = context_elem.find('.//xbrli:period/xbrli:instant', self.namespaces)
                start = context_elem.find('.//xbrli:period/xbrli:startDate', self.namespaces)
                end = context_elem.find('.//xbrli:period/xbrli:endDate', self.namespaces)
                
                if instant is not None:
                    period = {'instant': instant.text}
                elif start is not None and end is not None:
                    period = {'start': start.text, 'end': end.text}
                
                dimensions = {}
                for segment in context_elem.findall('.//xbrli:entity/xbrli:segment', self.namespaces):
                    dims = self._extract_dimensions(segment)
                    dimensions.update(dims)
                
                for scenario in context_elem.findall('.//xbrli:scenario', self.namespaces):
                    dims = self._extract_dimensions(scenario)
                    dimensions.update(dims)
                
                contexts.append({
                    'contextId': context_id,
                    'entityIdentifier': entity_id,
                    'period': period,
                    'scenario': None,
                    'dimensions': dimensions if dimensions else None
                })
            except Exception as e:
                self.logger.warning("context_extraction_error", error=str(e))
                continue
        
        self.logger.info("contexts_extracted", count=len(contexts))
        return contexts
    
    def _extract_dimensions(self, parent_elem) -> Dict:
        """Extract dimension members from segment or scenario."""
        dimensions = {}
        
        for dim_elem in parent_elem.findall('.//xbrldi:explicitMember', self.namespaces):
            dim_attr = dim_elem.get('dimension')
            member_value = dim_elem.text
            
            if dim_attr:
                dim_name = dim_attr.split(':')[-1] if ':' in dim_attr else dim_attr
                dimensions[dim_name] = member_value
        
        return dimensions
    
    def _extract_units(self, root) -> List[Dict]:
        """Extract all units from XBRL instance."""
        units = []
        
        for unit_elem in root.findall('.//xbrli:unit', self.namespaces):
            try:
                unit_id = unit_elem.get('id')
                
                measures = []
                for measure in unit_elem.findall('.//xbrli:measure', self.namespaces):
                    if measure.text:
                        measures.append(measure.text)
                
                unit_type = 'other'
                for measure in measures:
                    if 'currency' in measure.lower() or any(curr in measure.upper() for curr in ['USD', 'EUR', 'GBP']):
                        unit_type = 'monetary'
                        break
                    elif 'shares' in measure.lower():
                        unit_type = 'shares'
                        break
                
                units.append({
                    'unitId': unit_id,
                    'measures': measures,
                    'type': unit_type
                })
            except Exception as e:
                self.logger.warning("unit_extraction_error", error=str(e))
                continue
        
        self.logger.info("units_extracted", count=len(units))
        return units
    
    def _extract_facts(self, root, contexts: List[Dict], units: List[Dict]) -> List[Dict]:
        """Extract all facts from XBRL instance."""
        facts = []
        context_map = {c['contextId']: c for c in contexts}
        
        inline_ns = {
            'ix': 'http://www.xbrl.org/2013/inlineXBRL',
            'ixt': 'http://www.xbrl.org/inlineXBRL/transformation/2015-02-26',
            'xbrli': 'http://www.xbrl.org/2003/instance',
        }
        
        all_ns = {**self.namespaces, **inline_ns}
        
        for elem in root.iter():
            tag = elem.tag
            
            is_inline_numeric = tag.endswith('}nonFraction') or tag.endswith('}nonNumeric')
            is_inline_text = tag.endswith('}nonNumeric')
            
            if is_inline_numeric or is_inline_text:
                try:
                    context_ref = elem.get('contextRef')
                    unit_ref = elem.get('unitRef')
                    decimals = elem.get('decimals')
                    
                    concept_name = elem.get('name')
                    if not concept_name or not context_ref:
                        continue
                    
                    value = elem.text if elem.text else None
                    
                    if value and is_inline_numeric:
                        try:
                            value = value.replace(',', '').replace('$', '').strip()
                            if value and value not in ['-', '—']:
                                value = float(value)
                        except:
                            pass
                    
                    if ':' in concept_name:
                        prefix, local_name = concept_name.split(':', 1)
                        namespace = self.namespaces.get(prefix)
                    else:
                        local_name = concept_name
                        namespace = None
                    
                    facts.append({
                        'concept': {
                            'name': concept_name,
                            'label': self._humanize_label(local_name),
                            'namespace': namespace
                        },
                        'value': value,
                        'unitRef': unit_ref,
                        'decimals': decimals,
                        'contextRef': context_ref,
                        'isNil': value is None,
                        'footnotes': []
                    })
                except Exception as e:
                    self.logger.debug("inline_fact_extraction_error", error=str(e))
                    continue
        
        if len(facts) == 0:
            for elem in root.iter():
                if elem.tag.startswith('{http://www.xbrl.org/'):
                    continue
                
                try:
                    context_ref = elem.get('contextRef')
                    unit_ref = elem.get('unitRef')
                    decimals = elem.get('decimals')
                    
                    if not context_ref:
                        continue
                    
                    is_nil = elem.get('{http://www.w3.org/2001/XMLSchema-instance}nil') == 'true'
                    value = None if is_nil else elem.text
                    
                    if value and not is_nil:
                        try:
                            value = value.replace(',', '').strip()
                            if decimals and decimals != 'INF':
                                value = float(value)
                            elif value.replace('.', '').replace('-', '').isdigit():
                                value = float(value)
                        except:
                            pass
                    
                    tag_name = elem.tag
                    namespace = tag_name.split('}')[0][1:] if '}' in tag_name else None
                    local_name = tag_name.split('}')[1] if '}' in tag_name else tag_name
                    
                    prefix = None
                    for p, uri in self.namespaces.items():
                        if uri == namespace:
                            prefix = p
                            break
                    
                    concept_name = f"{prefix}:{local_name}" if prefix else local_name
                    
                    facts.append({
                        'concept': {
                            'name': concept_name,
                            'label': self._humanize_label(local_name),
                            'namespace': namespace
                        },
                        'value': value,
                        'unitRef': unit_ref,
                        'decimals': decimals,
                        'contextRef': context_ref,
                        'isNil': is_nil,
                        'footnotes': []
                    })
                except Exception as e:
                    self.logger.debug("fact_extraction_error", tag=elem.tag, error=str(e))
                    continue
        
        self.logger.info("facts_extracted", count=len(facts))
        return facts
    
    def _humanize_label(self, camel_case: str) -> str:
        """Convert camelCase to human readable label."""
        result = re.sub(r'([A-Z])', r' \1', camel_case)
        return result.strip().title()
    
    def _extract_footnotes(self, root) -> List[Dict]:
        """Extract footnotes from XBRL instance."""
        footnotes = []
        
        for footnote_link in root.findall('.//link:footnoteLink', self.namespaces):
            for footnote in footnote_link.findall('.//link:footnote', self.namespaces):
                try:
                    footnote_id = footnote.get('{http://www.w3.org/1999/xlink}label')
                    text = ''.join(footnote.itertext()).strip()
                    
                    footnotes.append({
                        'id': footnote_id,
                        'text': text,
                        'role': footnote.get('{http://www.w3.org/1999/xlink}role')
                    })
                except Exception as e:
                    self.logger.warning("footnote_extraction_error", error=str(e))
                    continue
        
        self.logger.info("footnotes_extracted", count=len(footnotes))
        return footnotes


class MongoDBStorage:
    """MongoDB storage handler for SEC filings."""
    
    def __init__(self):
        self.client = MongoClient(CONFIG['MONGODB_URI'])
        self.db = self.client[CONFIG['DATABASE_NAME']]
        self.logger = get_logger(f"{__name__}.MongoDBStorage")
        self._create_indexes()
    
    def _create_indexes(self):
        """Create recommended indexes for efficient querying."""
        try:
            existing_indexes = self.db.filings.index_information()
            old_indexes = ['idx_ticker_docid_unique', 'metadata.ticker_1_metadata.doc_id_1']
            for old_idx in old_indexes:
                if old_idx in existing_indexes:
                    self.logger.warning("dropping_old_index", index=old_idx)
                    self.db.filings.drop_index(old_idx)
            
            null_count = self.db.filings.count_documents({'accessionNumber': None})
            if null_count > 0:
                self.logger.warning("removing_null_filings", count=null_count)
                self.db.filings.delete_many({'accessionNumber': None})
            
            old_schema_count = self.db.filings.count_documents({'metadata': {'$exists': True}})
            if old_schema_count > 0:
                self.logger.warning("removing_old_schema_filings", count=old_schema_count)
                self.db.filings.delete_many({'metadata': {'$exists': True}})
            
            self.db.filings.create_index([('cik', ASCENDING), ('periodEnd', DESCENDING)])
            self.db.filings.create_index([('ticker', ASCENDING), ('filingDate', DESCENDING)])
            if 'accessionNumber_1' not in self.db.filings.index_information():
                self.db.filings.create_index([('accessionNumber', ASCENDING)], unique=True)
            
            self.db.facts.create_index([('concept.name', ASCENDING), ('company.cik', ASCENDING), ('period.end', DESCENDING)])
            self.db.facts.create_index([('filingId', ASCENDING)])
            self.db.facts.create_index([('company.ticker', ASCENDING), ('concept.name', ASCENDING)])
            
            self.db.contexts.create_index([('contextId', ASCENDING), ('filingId', ASCENDING)])
            
            self.db.units.create_index([('unitId', ASCENDING), ('filingId', ASCENDING)])
            
            self.logger.info("indexes_created", status="success")
        except Exception as e:
            self.logger.warning("index_creation_warning", error=str(e))
    
    def store_filing(self, filing_metadata: Dict, xbrl_data: Dict) -> Optional[ObjectId]:
        """Store a complete filing with all related data."""
        try:
            filing_doc = {
                'accessionNumber': filing_metadata['accessionNumber'],
                'cik': filing_metadata['cik'],
                'ticker': filing_metadata['ticker'],
                'formType': filing_metadata['formType'],
                'filingDate': filing_metadata['filingDate'],
                'periodEnd': self._extract_period_end(xbrl_data['contexts']),
                'sourceUrl': filing_metadata['documentUrl'],
                'acceptedDate': filing_metadata['filingDate'] + 'T00:00:00Z',
                'createdAt': datetime.now(UTC)
            }
            
            try:
                result = self.db.filings.insert_one(filing_doc)
                filing_id = result.inserted_id
                self.logger.info("filing_stored", accession=filing_metadata['accessionNumber'], filing_id=str(filing_id))
            except DuplicateKeyError:
                self.logger.warning("filing_exists", accession=filing_metadata['accessionNumber'])
                existing = self.db.filings.find_one({'accessionNumber': filing_metadata['accessionNumber']})
                if existing:
                    filing_id = existing['_id']
                    self.logger.info("using_existing_filing", filing_id=str(filing_id))
                    return filing_id
                else:
                    self.logger.error("existing_filing_not_found", accession=filing_metadata['accessionNumber'])
                    return None
            
            self._store_contexts(filing_id, xbrl_data['contexts'])
            self._store_units(filing_id, xbrl_data['units'])
            self._store_facts(filing_id, filing_metadata, xbrl_data)
            
            return filing_id
        except Exception as e:
            self.logger.error("filing_store_error", error=str(e), exc_info=True)
            return None
    
    def _extract_period_end(self, contexts: List[Dict]) -> Optional[str]:
        """Extract the latest period end date from contexts."""
        period_ends = []
        for ctx in contexts:
            if ctx['period'].get('end'):
                period_ends.append(ctx['period']['end'])
            elif ctx['period'].get('instant'):
                period_ends.append(ctx['period']['instant'])
        
        return max(period_ends) if period_ends else None
    
    def _store_contexts(self, filing_id: ObjectId, contexts: List[Dict]):
        """Store contexts for a filing."""
        if not contexts:
            return
        
        try:
            context_docs = []
            for ctx in contexts:
                doc = ctx.copy()
                doc['filingId'] = filing_id
                context_docs.append(doc)
            
            if context_docs:
                self.db.contexts.insert_many(context_docs, ordered=False)
                self.logger.info("contexts_stored", filing_id=str(filing_id), count=len(context_docs))
        except Exception as e:
            self.logger.warning("contexts_store_error", error=str(e))
    
    def _store_units(self, filing_id: ObjectId, units: List[Dict]):
        """Store units for a filing."""
        if not units:
            return
        
        try:
            unit_docs = []
            for unit in units:
                doc = unit.copy()
                doc['filingId'] = filing_id
                unit_docs.append(doc)
            
            if unit_docs:
                self.db.units.insert_many(unit_docs, ordered=False)
                self.logger.info("units_stored", filing_id=str(filing_id), count=len(unit_docs))
        except Exception as e:
            self.logger.warning("units_store_error", error=str(e))
    
    def _store_facts(self, filing_id: ObjectId, filing_metadata: Dict, xbrl_data: Dict):
        """Store facts with enriched context and period data."""
        if not xbrl_data['facts']:
            return
        
        try:
            context_map = {ctx['contextId']: ctx for ctx in xbrl_data['contexts']}
            
            fact_docs = []
            for fact in xbrl_data['facts']:
                context = context_map.get(fact['contextRef'], {})
                period = context.get('period', {})
                dimensions = context.get('dimensions')
                
                doc = {
                    'filingId': filing_id,
                    'company': {
                        'cik': filing_metadata['cik'],
                        'ticker': filing_metadata['ticker']
                    },
                    'concept': fact['concept'],
                    'value': fact['value'],
                    'unitRef': fact['unitRef'],
                    'decimals': fact['decimals'],
                    'contextRef': fact['contextRef'],
                    'period': period if period else None,
                    'dimensions': dimensions,
                    'footnotes': fact.get('footnotes', []),
                    'isNil': fact['isNil']
                }
                
                fact_docs.append(doc)
            
            if fact_docs:
                batch_size = 1000
                for i in range(0, len(fact_docs), batch_size):
                    batch = fact_docs[i:i+batch_size]
                    self.db.facts.insert_many(batch, ordered=False)
                
                self.logger.info("facts_stored", filing_id=str(filing_id), count=len(fact_docs))
        except Exception as e:
            self.logger.error("facts_store_error", error=str(e), exc_info=True)
    
    def get_latest_fact(self, ticker: str, concept_name: str) -> Optional[Dict]:
        """Query the latest fact for a ticker and concept."""
        try:
            result = self.db.facts.find_one(
                {
                    'company.ticker': ticker,
                    'concept.name': concept_name
                },
                sort=[('period.end', DESCENDING)]
            )
            return result
        except Exception as e:
            self.logger.error("fact_query_error", error=str(e))
            return None


class SECFilingPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self):
        self.edgar_client = SECEdgarClient()
        self.xbrl_parser = XBRLParser()
        self.storage = MongoDBStorage()
        self.logger = get_logger(f"{__name__}.SECFilingPipeline")
    
    def process_ticker(self, ticker: str) -> bool:
        """Process a single ticker through the entire pipeline."""
        try:
            self.logger.info("processing_ticker", ticker=ticker)
            
            cik = self.edgar_client.get_cik_for_ticker(ticker)
            if not cik:
                self.logger.error("cik_not_found", ticker=ticker)
                return False
            
            filing = self.edgar_client.get_latest_10q_filing(cik, ticker)
            if not filing:
                self.logger.error("no_10q_filing", ticker=ticker)
                return False
            
            xbrl_paths = self.edgar_client.download_xbrl_package(filing)
            if not xbrl_paths:
                self.logger.error("xbrl_download_failed", ticker=ticker)
                return False
            
            xbrl_data = self.xbrl_parser.parse_filing(xbrl_paths)
            if not xbrl_data:
                self.logger.error("xbrl_parse_failed", ticker=ticker)
                return False
            
            filing_id = self.storage.store_filing(filing, xbrl_data)
            if not filing_id:
                self.logger.error("store_failed", ticker=ticker)
                return False
            
            self.logger.info("ticker_processed", ticker=ticker, filing_id=str(filing_id), status="success")
            return True
        except Exception as e:
            self.logger.error("ticker_processing_error", ticker=ticker, error=str(e), exc_info=True)
            return False
    
    def run(self, tickers: List[str]):
        """Run the pipeline for all tickers."""
        self.logger.info("pipeline_started", ticker_count=len(tickers))
        
        success_count = 0
        failed_tickers = []
        
        for i, ticker in enumerate(tickers, 1):
            try:
                self.logger.info("processing_progress", current=i, total=len(tickers), ticker=ticker)
                if self.process_ticker(ticker):
                    success_count += 1
                else:
                    failed_tickers.append(ticker)
            except Exception as e:
                self.logger.error("unexpected_ticker_error", ticker=ticker, error=str(e), exc_info=True)
                failed_tickers.append(ticker)
        
        self.logger.info("pipeline_completed", success=success_count, total=len(tickers), failed=len(failed_tickers))
        if failed_tickers:
            self.logger.warning("failed_tickers", tickers=failed_tickers, count=len(failed_tickers))


def clean_database():
    """Clean up database - use this if you have corrupted data."""
    client = MongoClient(CONFIG['MONGODB_URI'])
    db = client[CONFIG['DATABASE_NAME']]
    
    logger.info("database_cleanup_started")
    
    collections = ['filings', 'facts', 'contexts', 'units']
    for collection in collections:
        count = db[collection].count_documents({})
        if count > 0:
            response = input(f"Delete {count} documents from {collection}? (yes/no): ")
            if response.lower() == 'yes':
                db[collection].drop()
                logger.info("collection_dropped", collection=collection)
    
    logger.info("database_cleanup_completed")
    client.close()


def run_daemon_mode():
    """Run pipeline continuously as a background microservice."""
    import signal
    import os
    
    fetch_interval = int(os.getenv('FETCH_INTERVAL', 3600))
    max_tickers = int(os.getenv('MAX_TICKERS_PER_RUN', 10))
    
    logger.info("daemon_started", fetch_interval=fetch_interval, max_tickers=max_tickers, mongodb_uri=CONFIG['MONGODB_URI'])
    
    pipeline = SECFilingPipeline()
    
    shutdown_flag = False
    
    def signal_handler(signum, frame):
        nonlocal shutdown_flag
        logger.info("shutdown_signal_received", signal=signum)
        shutdown_flag = True
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    cycle_count = 0
    
    while not shutdown_flag:
        try:
            cycle_count += 1
            logger.info("cycle_started", cycle=cycle_count, timestamp=datetime.now(UTC).isoformat())
            
            tickers_to_process = TICKERS[:max_tickers]
            
            logger.info("processing_tickers", count=len(tickers_to_process))
            pipeline.run(tickers_to_process)
            
            if not shutdown_flag:
                next_run = (datetime.now(UTC) + timedelta(seconds=fetch_interval)).isoformat()
                logger.info("waiting_for_next_cycle", wait_seconds=fetch_interval, next_run=next_run)
                
                for _ in range(fetch_interval):
                    if shutdown_flag:
                        break
                    time.sleep(1)
        
        except Exception as e:
            logger.error("daemon_cycle_error", error=str(e), exc_info=True)
            
            if not shutdown_flag:
                logger.info("retry_wait", wait_seconds=60)
                time.sleep(60)
    
    logger.info("daemon_shutdown_complete")


def run_single_batch(tickers_input: str, limit: int = 1):
    """Run pipeline once for specified tickers (for manual/cron execution)."""
    logger.info("batch_mode_started")
    
    if tickers_input.upper() == 'ALL':
        tickers = TICKERS
    else:
        tickers = [t.strip().upper() for t in tickers_input.split(',')]
    
    logger.info("processing_batch", ticker_count=len(tickers), tickers=tickers[:5])
    
    pipeline = SECFilingPipeline()
    pipeline.run(tickers)
    
    logger.info("batch_mode_completed")


def main():
    """Main entry point with multiple run modes."""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(
        description='SEC Filings XBRL Pipeline - Microservice Mode',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run as continuous microservice (recommended for Docker)
  python sec_filings.py --daemon
  
  # Process specific tickers once
  python sec_filings.py --tickers AAPL,MSFT,GOOGL
  
  # Process all tickers once
  python sec_filings.py --tickers ALL
  
  # Clean database
  python sec_filings.py --clean
        """
    )
    
    parser.add_argument('--daemon', action='store_true',
                       help='Run as continuous background microservice')
    parser.add_argument('--tickers', type=str,
                       help='Comma-separated ticker symbols or "ALL"')
    parser.add_argument('--limit', type=int, default=1,
                       help='Number of filings per ticker (default: 1)')
    parser.add_argument('--clean', action='store_true',
                       help='Clean database (interactive)')
    
    args = parser.parse_args()
    
    if args.clean:
        clean_database()
        return
    
    if args.daemon or os.getenv('DAEMON_MODE', '').lower() == 'true':
        run_daemon_mode()
        return
    
    if args.tickers:
        run_single_batch(args.tickers, args.limit)
        return
    
    parser.print_help()
    logger.warning("no_mode_specified")


if __name__ == '__main__':
    main()