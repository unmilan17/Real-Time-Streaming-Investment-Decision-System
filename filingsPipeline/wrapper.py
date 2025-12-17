#!/usr/bin/env python3
"""
Enhanced CLI Wrapper for SEC Filing Pipeline
Provides beautiful command-line interface with interactive querying
"""

import argparse
import sys
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime
from pymongo import MongoClient, DESCENDING
import json
from dotenv import load_dotenv

load_dotenv()
# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DIM = '\033[2m'


class ProgressIndicator:
    """Animated progress indicator for operations."""
    
    def __init__(self, message: str):
        self.message = message
        self.spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        self.idx = 0
    
    def show(self):
        """Show next frame of spinner."""
        sys.stdout.write(f'\r{Colors.CYAN}{self.spinner[self.idx]} {self.message}...{Colors.ENDC}')
        sys.stdout.flush()
        self.idx = (self.idx + 1) % len(self.spinner)
    
    def stop(self, success: bool = True):
        """Stop spinner and show result."""
        symbol = '✓' if success else '✗'
        color = Colors.GREEN if success else Colors.FAIL
        sys.stdout.write(f'\r{color}{symbol} {self.message}{Colors.ENDC}\n')
        sys.stdout.flush()


def print_banner():
    """Print enhanced startup banner."""
    banner = f"""
{Colors.BOLD}{Colors.CYAN}╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║              🏦 SEC FILING PIPELINE - INTERACTIVE CLI 📊              ║
║                                                                      ║
║           XBRL Parser & MongoDB Storage System                       ║
║           Powered by SEC EDGAR API + Arelle Parser                   ║
║                                                                      ║
║  📥 Fetches 10-Q/10-K filings from SEC EDGAR                         ║
║  🔍 Parses XBRL financial data using advanced parser                ║
║  💾 Stores structured data in MongoDB                                ║
║  📊 Interactive querying and visualization                           ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝{Colors.ENDC}
    """
    print(banner)


def print_section_header(title: str, icon: str = "▶️"):
    """Print a beautiful section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{icon} {title}{Colors.ENDC}")
    print(f"{Colors.BLUE}{'─' * 74}{Colors.ENDC}")


def print_box(title: str, items: List[str], color=Colors.GREEN):
    """Print content in a bordered box."""
    max_len = max(len(item) for item in items) if items else 0
    max_len = max(max_len, len(title))
    border = "═" * (max_len + 4)
    
    print(f"\n{color}{Colors.BOLD}╔{border}╗")
    print(f"║  {title}{' ' * (max_len - len(title))}  ║")
    print(f"╠{border}╣")
    for item in items:
        padding = " " * (max_len - len(item))
        print(f"║  {item}{padding}  ║")
    print(f"╚{border}╝{Colors.ENDC}\n")


def format_number(value) -> str:
    """Format numbers with commas and proper decimals."""
    try:
        num = float(value)
        if abs(num) >= 1_000_000_000:
            return f"${num/1_000_000_000:.2f}B"
        elif abs(num) >= 1_000_000:
            return f"${num/1_000_000:.2f}M"
        elif abs(num) >= 1_000:
            return f"${num/1_000:.2f}K"
        else:
            return f"${num:,.2f}"
    except:
        return str(value)


def format_table(headers: List[str], rows: List[List[str]], col_widths: Optional[List[int]] = None):
    """Print a beautiful ASCII table."""
    if not col_widths:
        col_widths = [max(len(str(row[i])) for row in [headers] + rows) for i in range(len(headers))]
    
    # Top border
    print(f"\n{Colors.CYAN}┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐{Colors.ENDC}")
    
    # Headers
    header_row = "│".join(f" {Colors.BOLD}{h:<{col_widths[i]}}{Colors.ENDC} " for i, h in enumerate(headers))
    print(f"{Colors.CYAN}│{Colors.ENDC}{header_row}{Colors.CYAN}│{Colors.ENDC}")
    
    # Separator
    print(f"{Colors.CYAN}├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤{Colors.ENDC}")
    
    # Rows
    for row in rows:
        row_str = "│".join(f" {str(row[i]):<{col_widths[i]}} " for i in range(len(row)))
        print(f"{Colors.CYAN}│{Colors.ENDC}{row_str}{Colors.CYAN}│{Colors.ENDC}")
    
    # Bottom border
    print(f"{Colors.CYAN}└" + "┴".join("─" * (w + 2) for w in col_widths) + "┘{Colors.ENDC}\n")


class QueryInterface:
    """Interactive query interface for MongoDB data."""
    
    def __init__(self, db_uri: str = 'mongodb://localhost:27017/', db_name: str = 'sec_filings'):
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
    
    def get_database_stats(self) -> Dict:
        """Get overall database statistics."""
        return {
            'filings': self.db.filings.count_documents({}),
            'facts': self.db.facts.count_documents({}),
            'contexts': self.db.contexts.count_documents({}),
            'units': self.db.units.count_documents({})
        }
    
    def get_available_tickers(self) -> List[str]:
        """Get list of available tickers."""
        return sorted(self.db.filings.distinct('ticker'))
    
    def get_ticker_summary(self, ticker: str) -> Dict:
        """Get summary for a specific ticker."""
        filings = list(self.db.filings.find({'ticker': ticker}))
        if not filings:
            return None
        
        latest_filing = filings[0]
        facts_count = self.db.facts.count_documents({'company.ticker': ticker})
        
        return {
            'ticker': ticker,
            'cik': latest_filing.get('cik'),
            'filings_count': len(filings),
            'facts_count': facts_count,
            'latest_filing_date': latest_filing.get('filingDate'),
            'form_type': latest_filing.get('formType')
        }
    
    def query_revenue(self, ticker: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Query revenue facts."""
        query = {
            'concept.name': {'$regex': 'Revenue', '$options': 'i'},
            'value': {'$ne': None, '$ne': 0}
        }
        if ticker:
            query['company.ticker'] = ticker.upper()
        
        return list(self.db.facts.find(query).sort('value', -1).limit(limit))
    
    def query_assets(self, ticker: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Query asset facts."""
        query = {
            'concept.name': {'$regex': 'Assets', '$options': 'i'},
            'value': {'$ne': None, '$ne': 0}
        }
        if ticker:
            query['company.ticker'] = ticker.upper()
        
        return list(self.db.facts.find(query).sort('value', -1).limit(limit))
    
    def query_concept(self, concept_name: str, ticker: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Query specific concept."""
        query = {
            'concept.name': {'$regex': concept_name, '$options': 'i'},
            'value': {'$ne': None}
        }
        if ticker:
            query['company.ticker'] = ticker.upper()
        
        return list(self.db.facts.find(query).limit(limit))
    
    def get_common_concepts(self, limit: int = 15) -> List[Dict]:
        """Get most common financial concepts."""
        pipeline = [
            {'$group': {
                '_id': '$concept.name',
                'count': {'$sum': 1},
                'companies': {'$addToSet': '$company.ticker'},
                'sample_value': {'$first': '$value'}
            }},
            {'$sort': {'count': -1}},
            {'$limit': limit}
        ]
        return list(self.db.facts.aggregate(pipeline))
    
    def compare_metrics(self, concept_name: str, tickers: List[str]) -> List[Dict]:
        """Compare a metric across multiple companies."""
        results = []
        for ticker in tickers:
            fact = self.db.facts.find_one(
                {
                    'company.ticker': ticker.upper(),
                    'concept.name': {'$regex': concept_name, '$options': 'i'},
                    'value': {'$ne': None}
                },
                sort=[('period.end', -1)]
            )
            if fact:
                results.append(fact)
        return results
    
    def close(self):
        """Close database connection."""
        self.client.close()


def display_database_overview(query_interface: QueryInterface):
    """Display beautiful database overview."""
    print_section_header("📊 Database Overview", "📊")
    
    stats = query_interface.get_database_stats()
    tickers = query_interface.get_available_tickers()
    
    # Stats box
    stats_items = [
        f"📄 Filings:       {stats['filings']:>10,} documents",
        f"📊 Facts:         {stats['facts']:>10,} documents",
        f"📅 Contexts:      {stats['contexts']:>10,} documents",
        f"🔢 Units:         {stats['units']:>10,} documents",
        f"{'─' * 42}",
        f"💾 Total:         {sum(stats.values()):>10,} documents",
        f"",
        f"🏢 Companies:     {len(tickers)} ticker(s) available",
        f"   {', '.join(tickers)}"
    ]
    
    print_box("DATABASE STATISTICS", stats_items, Colors.CYAN)


def display_ticker_info(query_interface: QueryInterface, ticker: str):
    """Display detailed ticker information."""
    print_section_header(f"🏢 Company Information: {ticker}", "🏢")
    
    summary = query_interface.get_ticker_summary(ticker)
    if not summary:
        print(f"{Colors.FAIL}✗ No data found for ticker: {ticker}{Colors.ENDC}\n")
        return
    
    info_items = [
        f"Ticker Symbol:    {summary['ticker']}",
        f"CIK Code:         {summary['cik']}",
        f"Total Filings:    {summary['filings_count']}",
        f"Total Facts:      {summary['facts_count']:,}",
        f"Latest Filing:    {summary['latest_filing_date']}",
        f"Form Type:        {summary['form_type']}"
    ]
    
    print_box(f"{ticker} OVERVIEW", info_items, Colors.GREEN)


def display_query_results(title: str, results: List[Dict], show_values: bool = True):
    """Display query results in a beautiful table."""
    print_section_header(title, "🔍")
    
    if not results:
        print(f"{Colors.WARNING}No results found{Colors.ENDC}\n")
        return
    
    print(f"{Colors.DIM}Found {len(results)} result(s){Colors.ENDC}\n")
    
    # Prepare table data
    headers = ["#", "Ticker", "Concept", "Value", "Period"]
    rows = []
    
    for i, doc in enumerate(results, 1):
        ticker = doc.get('company', {}).get('ticker', 'N/A')
        concept = doc.get('concept', {}).get('name', 'N/A').split(':')[-1]
        value = doc.get('value', 'N/A')
        period = doc.get('period', {})
        period_str = period.get('end', period.get('instant', 'N/A'))
        
        # Format concept name (truncate if too long)
        if len(concept) > 30:
            concept = concept[:27] + "..."
        
        # Format value
        if show_values and value != 'N/A':
            value_str = format_number(value)
        else:
            value_str = str(value)[:15]
        
        rows.append([str(i), ticker, concept, value_str, period_str])
    
    format_table(headers, rows)


def display_concept_ranking(results: List[Dict]):
    """Display concept ranking table."""
    print_section_header("📈 Most Common Financial Concepts", "📈")
    
    if not results:
        print(f"{Colors.WARNING}No results found{Colors.ENDC}\n")
        return
    
    headers = ["Rank", "Concept", "Count", "Companies"]
    rows = []
    
    for i, doc in enumerate(results, 1):
        concept = doc['_id'].split(':')[-1] if ':' in doc['_id'] else doc['_id']
        if len(concept) > 35:
            concept = concept[:32] + "..."
        
        companies = ", ".join(doc['companies'][:3])
        if len(doc['companies']) > 3:
            companies += f" +{len(doc['companies'])-3}"
        
        rows.append([
            f"#{i}",
            concept,
            str(doc['count']),
            companies
        ])
    
    format_table(headers, rows, col_widths=[5, 35, 8, 30])


def display_comparison(title: str, results: List[Dict]):
    """Display comparison results."""
    print_section_header(title, "⚖️")
    
    if not results:
        print(f"{Colors.WARNING}No results found for comparison{Colors.ENDC}\n")
        return
    
    headers = ["Ticker", "Concept", "Value", "Period"]
    rows = []
    
    for doc in results:
        ticker = doc.get('company', {}).get('ticker', 'N/A')
        concept = doc.get('concept', {}).get('name', 'N/A').split(':')[-1]
        value = format_number(doc.get('value', 'N/A'))
        period = doc.get('period', {}).get('end', 'N/A')
        
        rows.append([ticker, concept[:30], value, period])
    
    format_table(headers, rows, col_widths=[8, 30, 18, 12])


def interactive_query_mode(query_interface: QueryInterface):
    """Run interactive query mode."""
    print_section_header("🎯 Interactive Query Mode", "🎯")
    
    print(f"\n{Colors.BOLD}Available Commands:{Colors.ENDC}")
    print(f"  {Colors.CYAN}1{Colors.ENDC}  - Show database overview")
    print(f"  {Colors.CYAN}2{Colors.ENDC}  - Query revenue data")
    print(f"  {Colors.CYAN}3{Colors.ENDC}  - Query asset data")
    print(f"  {Colors.CYAN}4{Colors.ENDC}  - Search by concept name")
    print(f"  {Colors.CYAN}5{Colors.ENDC}  - Show common concepts")
    print(f"  {Colors.CYAN}6{Colors.ENDC}  - Compare metrics across companies")
    print(f"  {Colors.CYAN}7{Colors.ENDC}  - Show company details")
    print(f"  {Colors.CYAN}q{Colors.ENDC}  - Quit\n")
    
    while True:
        try:
            choice = input(f"{Colors.BOLD}Query > {Colors.ENDC}").strip().lower()
            
            if choice == 'q':
                print(f"\n{Colors.GREEN}👋 Goodbye!{Colors.ENDC}\n")
                break
            
            elif choice == '1':
                display_database_overview(query_interface)
            
            elif choice == '2':
                ticker = input(f"  Enter ticker (or press Enter for all): ").strip().upper() or None
                results = query_interface.query_revenue(ticker, limit=10)
                display_query_results("💰 Revenue Data", results)
            
            elif choice == '3':
                ticker = input(f"  Enter ticker (or press Enter for all): ").strip().upper() or None
                results = query_interface.query_assets(ticker, limit=10)
                display_query_results("🏦 Asset Data", results)
            
            elif choice == '4':
                concept = input(f"  Enter concept name (e.g., 'Cash', 'Liabilities'): ").strip()
                ticker = input(f"  Enter ticker (or press Enter for all): ").strip().upper() or None
                results = query_interface.query_concept(concept, ticker, limit=10)
                display_query_results(f"🔍 Search: {concept}", results)
            
            elif choice == '5':
                results = query_interface.get_common_concepts(limit=15)
                display_concept_ranking(results)
            
            elif choice == '6':
                concept = input(f"  Enter concept to compare (e.g., 'Revenue'): ").strip()
                tickers_str = input(f"  Enter tickers separated by commas (e.g., AAPL,MSFT): ").strip()
                tickers = [t.strip().upper() for t in tickers_str.split(',') if t.strip()]
                results = query_interface.compare_metrics(concept, tickers)
                display_comparison(f"⚖️  Comparing: {concept}", results)
            
            elif choice == '7':
                ticker = input(f"  Enter ticker: ").strip().upper()
                display_ticker_info(query_interface, ticker)
            
            else:
                print(f"{Colors.WARNING}Invalid choice. Please try again.{Colors.ENDC}\n")
        
        except KeyboardInterrupt:
            print(f"\n\n{Colors.GREEN}👋 Goodbye!{Colors.ENDC}\n")
            break
        except Exception as e:
            print(f"\n{Colors.FAIL}Error: {e}{Colors.ENDC}\n")


def run_auto_queries(query_interface: QueryInterface):
    """Run automatic demonstration queries."""
    print_section_header("🎬 Running Demonstration Queries", "🎬")
    
    # Overview
    display_database_overview(query_interface)
    time.sleep(1)
    
    # Top revenue
    results = query_interface.query_revenue(limit=10)
    display_query_results("💰 Top Revenue Facts", results)
    time.sleep(1)
    
    # Common concepts
    results = query_interface.get_common_concepts(limit=10)
    display_concept_ranking(results)
    time.sleep(1)
    
    # Company-specific
    tickers = query_interface.get_available_tickers()
    if tickers:
        ticker = tickers[0]
        display_ticker_info(query_interface, ticker)
        
        results = query_interface.query_revenue(ticker, limit=5)
        display_query_results(f"💰 Revenue for {ticker}", results)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='SEC Filing Pipeline - Enhanced Interactive CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{Colors.BOLD}Examples:{Colors.ENDC}
  {Colors.CYAN}# Process single ticker with interactive queries{Colors.ENDC}
  %(prog)s --ticker AAPL --interactive

  {Colors.CYAN}# Process multiple tickers and run auto-demo{Colors.ENDC}
  %(prog)s --tickers AAPL,MSFT --demo-queries

  {Colors.CYAN}# Just query existing data{Colors.ENDC}
  %(prog)s --query-only --interactive

  {Colors.CYAN}# Run with verification{Colors.ENDC}
  %(prog)s --ticker AAPL --verify
        """
    )
    
    # Ticker arguments
    ticker_group = parser.add_mutually_exclusive_group()
    ticker_group.add_argument('--ticker', type=str, help='Single ticker symbol')
    ticker_group.add_argument('--tickers', type=str, help='Comma-separated list of tickers')
    ticker_group.add_argument('--demo', action='store_true', help='Run demo mode')
    
    # Pipeline configuration
    parser.add_argument('--form-type', type=str, default='10-Q', help='Form type (default: 10-Q)')
    parser.add_argument('--limit', type=int, default=1, help='Filings per ticker (default: 1)')
    parser.add_argument('--cache-dir', type=str, default='./cache', help='Cache directory')
    
    # Query options
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive query mode')
    parser.add_argument('--demo-queries', action='store_true', help='Run demonstration queries')
    parser.add_argument('--query-only', action='store_true', help='Only run queries, skip pipeline')
    parser.add_argument('--verify', action='store_true', help='Verify data after ingestion')
    
    # Database operations
    parser.add_argument('--clean', action='store_true', help='Clean database before running')
    parser.add_argument('--clean-cache', action='store_true', help='Clean cache directory')
    
    # Output options
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode')
    
    args = parser.parse_args()
    
    # Setup
    if not args.quiet:
        print_banner()
    
    start_time = time.time()
    
    try:
        # Import pipeline if needed
        if not args.query_only:
            print_section_header("📦 Importing Pipeline Components", "📦")
            
            try:
                from sec_filings import SECFilingPipeline
                print(f"{Colors.GREEN}✓ Successfully imported SECFilingPipeline{Colors.ENDC}")
            except ImportError as e:
                print(f"{Colors.FAIL}✗ Failed to import: {e}{Colors.ENDC}")
                sys.exit(1)
            
            import os
            import shutil
            
            # Determine tickers
            tickers_to_process = []
            if args.demo:
                tickers_to_process = ['AAPL', 'MSFT']
            elif args.ticker:
                tickers_to_process = [args.ticker.upper()]
            elif args.tickers:
                tickers_to_process = [t.strip().upper() for t in args.tickers.split(',')]
            else:
                parser.error("Must specify --ticker, --tickers, --demo, or --query-only")
            
            # Show configuration
            print_section_header("⚙️  Pipeline Configuration", "⚙️")
            config_items = [
                f"Ticker(s):         {', '.join(tickers_to_process)}",
                f"Form Type:         {args.form_type}",
                f"Limit per Ticker:  {args.limit}",
                f"Cache Directory:   {args.cache_dir}",
            ]
            print_box("CONFIGURATION", config_items, Colors.BLUE)
            
            # Clean operations
            if args.clean or args.clean_cache:
                print_section_header("🧹 Cleanup Operations", "🧹")
                
                if args.clean:
                    progress = ProgressIndicator("Cleaning database")
                    for _ in range(3):
                        progress.show()
                        time.sleep(0.3)
                    
                    client = MongoClient('mongodb://localhost:27017/')
                    db = client['sec_filings']
                    for coll in ['filings', 'facts', 'contexts', 'units']:
                        db[coll].drop()
                    client.close()
                    
                    progress.stop(True)
                
                if args.clean_cache:
                    progress = ProgressIndicator("Cleaning cache")
                    for _ in range(3):
                        progress.show()
                        time.sleep(0.3)
                    
                    if os.path.exists(args.cache_dir):
                        shutil.rmtree(args.cache_dir)
                        os.makedirs(args.cache_dir, exist_ok=True)
                    
                    progress.stop(True)
            
            # Run pipeline
            print_section_header("🚀 Executing Pipeline", "🚀")
            print(f"\n{Colors.BOLD}Processing {len(tickers_to_process)} ticker(s)...{Colors.ENDC}\n")
            
            pipeline = SECFilingPipeline()
            
            for i, ticker in enumerate(tickers_to_process, 1):
                print(f"{Colors.CYAN}[{i}/{len(tickers_to_process)}] {ticker}{Colors.ENDC}")
                progress = ProgressIndicator(f"Fetching and parsing {ticker}")
                
                # Simulate progress (in real scenario, this would be in the pipeline)
                for _ in range(10):
                    progress.show()
                    time.sleep(0.2)
                
                success = pipeline.process_ticker(ticker)
                progress.stop(success)
            
            elapsed = time.time() - start_time
            
            success_msg = [
                "✓ Pipeline Completed Successfully!",
                f"  Processed: {len(tickers_to_process)} ticker(s)",
                f"  Elapsed Time: {elapsed:.2f} seconds"
            ]
            print_box("SUCCESS", success_msg, Colors.GREEN)
        
        # Query interface
        query_interface = QueryInterface()
        
        if args.verify or args.demo_queries:
            run_auto_queries(query_interface)
        
        if args.interactive:
            interactive_query_mode(query_interface)
        elif not args.query_only and not args.verify and not args.demo_queries:
            # Show quick summary
            display_database_overview(query_interface)
            print(f"\n{Colors.CYAN}💡 Tip: Use --interactive or --demo-queries to explore the data{Colors.ENDC}\n")
        
        query_interface.close()
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}⚠ Interrupted by user{Colors.ENDC}")
        sys.exit(130)
    except Exception as e:
        print(f"\n{Colors.FAIL}{'═' * 74}")
        print(f"Pipeline failed: {e}")
        print(f"{'═' * 74}{Colors.ENDC}\n")
        
        if args.verbose:
            import traceback
            traceback.print_exc()
        
        sys.exit(1)


if __name__ == '__main__':
    main()