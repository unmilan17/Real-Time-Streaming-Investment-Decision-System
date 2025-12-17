#!/bin/bash

################################################################################
# SEC Filing Pipeline - Enhanced Command Line Interface
# Beautiful CLI with interactive features and comprehensive verification
################################################################################

set -e
set -o pipefail

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'
DIM='\033[2m'

# Configuration
WRAPPER_SCRIPT="wrapper.py"
VENV_DIR="venv"
REQUIREMENTS_FILE="requirements.txt"
# Use MONGODB_URI from environment if set, otherwise default to localhost
MONGODB_URI="${MONGODB_URI:-mongodb://localhost:27017/}"
DATABASE_NAME="sec_filings"

################################################################################
# Beautiful Output Functions
################################################################################

print_banner() {
    cat << "EOF"

╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║              🏦 SEC FILING PIPELINE - LAUNCHER 📊                     ║
║                                                                      ║
║           Automated SEC EDGAR Data Pipeline                          ║
║           XBRL Parsing → MongoDB Storage → Interactive Queries       ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝

EOF
}

print_section() {
    echo -e "\n${BOLD}${BLUE}▶️ $1${NC}"
    echo -e "${BLUE}────────────────────────────────────────────────────────${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}" >&2
}

print_warning() {
    echo -e "${YELLOW}⚠ WARNING: $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

print_step() {
    echo -e "${MAGENTA}  → $1${NC}"
}

print_box() {
    local title="$1"
    shift
    local items=("$@")
    
    local max_len=0
    for item in "${items[@]}"; do
        local len=${#item}
        [ $len -gt $max_len ] && max_len=$len
    done
    [ ${#title} -gt $max_len ] && max_len=${#title}
    
    local border=$(printf '═%.0s' $(seq 1 $((max_len + 4))))
    
    echo -e "\n${GREEN}${BOLD}╔${border}╗"
    printf "║  ${title}%*s  ║\n" $((max_len - ${#title})) ""
    echo -e "╠${border}╣"
    for item in "${items[@]}"; do
        printf "║  ${item}%*s  ║\n" $((max_len - ${#item})) ""
    done
    echo -e "╚${border}╝${NC}\n"
}

spinner() {
    local pid=$1
    local message=$2
    local spinstr='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    local temp
    
    while kill -0 $pid 2>/dev/null; do
        temp=${spinstr#?}
        printf "\r${CYAN}%c ${message}...${NC}" "$spinstr"
        spinstr=$temp${spinstr%"$temp"}
        sleep 0.1
    done
    printf "\r${GREEN}✓ ${message}${NC}\n"
}

print_usage() {
    cat << EOF
${BOLD}Usage:${NC} $0 [OPTIONS]

${BOLD}Description:${NC}
  Complete SEC filing data pipeline with interactive querying capabilities.
  Fetches 10-Q/10-K filings, parses XBRL data, stores in MongoDB, and
  provides beautiful CLI for data exploration.

${BOLD}Pipeline Options:${NC}
  --ticker TICKER           Single ticker symbol (e.g., AAPL)
  --tickers LIST           Comma-separated tickers (e.g., AAPL,MSFT,GOOGL)
  --demo                   Run demo mode with AAPL,MSFT
  --form-type TYPE         Form type to fetch (default: 10-Q)
  --limit N                Filings per ticker (default: 1)

${BOLD}Query Options:${NC}
  --interactive, -i        Interactive query mode (recommended!)
  --demo-queries           Run demonstration queries automatically
  --query-only             Only query existing data (skip pipeline)
  --verify                 Run verification after pipeline

${BOLD}Database Options:${NC}
  --clean                  Clean database before running
  --clean-cache            Clean cache directory
  --clean-all              Clean both database and cache

${BOLD}Setup Options:${NC}
  --setup-only             Only setup environment
  --check-prereqs          Check prerequisites only

${BOLD}Output Options:${NC}
  --verbose, -v            Verbose output
  --quiet, -q              Suppress non-critical output
  --help, -h               Show this help

${BOLD}Examples:${NC}
  ${CYAN}# Quick demo (recommended for first run)${NC}
  $0 --demo --interactive

  ${CYAN}# Process single ticker with interactive queries${NC}
  $0 --ticker AAPL --interactive

  ${CYAN}# Process multiple tickers and show demo queries${NC}
  $0 --tickers AAPL,MSFT,GOOGL --demo-queries

  ${CYAN}# Query existing data without running pipeline${NC}
  $0 --query-only --interactive

  ${CYAN}# Clean start with verification${NC}
  $0 --clean-all --ticker AAPL --verify --interactive

  ${CYAN}# Process with verbose output${NC}
  $0 --ticker MSFT --verbose --demo-queries

EOF
}

################################################################################
# Environment Functions
################################################################################

check_prerequisites() {
    print_section "🔍 Checking Prerequisites"
    
    local all_good=true
    
    # Check Python
    if command -v python3 &> /dev/null; then
        local py_version=$(python3 --version 2>&1 | awk '{print $2}')
        print_success "Python 3 found: v${py_version}"
    else
        print_error "Python 3 not found"
        all_good=false
    fi
    
    # Check pip
    if command -v pip3 &> /dev/null; then
        print_success "pip3 found"
    else
        print_warning "pip3 not found (might cause issues)"
    fi
    
    # Check MongoDB
    print_step "Checking MongoDB connection..."
    if python3 -c "from pymongo import MongoClient; MongoClient('$MONGODB_URI', serverSelectionTimeoutMS=2000).admin.command('ping')" 2>/dev/null; then
        print_success "MongoDB accessible at $MONGODB_URI"
    else
        print_error "MongoDB not accessible at $MONGODB_URI"
        print_info "Please ensure MongoDB is running"
        all_good=false
    fi
    
    # Check wrapper script
    if [ -f "$WRAPPER_SCRIPT" ]; then
        print_success "Wrapper script found: $WRAPPER_SCRIPT"
    else
        print_error "Wrapper script not found: $WRAPPER_SCRIPT"
        all_good=false
    fi
    
    # Check requirements file
    if [ -f "$REQUIREMENTS_FILE" ]; then
        print_success "Requirements file found: $REQUIREMENTS_FILE"
    else
        print_warning "Requirements file not found: $REQUIREMENTS_FILE"
    fi
    
    echo ""
    
    if [ "$all_good" = true ]; then
        print_box "✓ All Prerequisites Met" "System ready to run pipeline"
        return 0
    else
        print_box "✗ Missing Prerequisites" "Please install missing components"
        return 1
    fi
}

setup_environment() {
    print_section "🔧 Setting Up Environment"
    
    # Create/activate virtual environment
    if [ -d "$VENV_DIR" ]; then
        print_info "Virtual environment exists: $VENV_DIR"
    else
        print_step "Creating virtual environment..."
        python3 -m venv "$VENV_DIR" &
        spinner $! "Creating virtual environment"
    fi
    
    print_step "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"
    print_success "Virtual environment activated"
    
    # Upgrade pip silently
    print_step "Upgrading pip..."
    pip install --quiet --upgrade pip &
    spinner $! "Upgrading pip"
    
    # Install dependencies
    if [ -f "$REQUIREMENTS_FILE" ]; then
        print_step "Installing dependencies..."
        pip install --quiet -r "$REQUIREMENTS_FILE" &
        spinner $! "Installing dependencies"
        
        # Verify key packages
        if python3 -c "import requests, pymongo, lxml" 2>/dev/null; then
            print_success "Key packages verified (requests, pymongo, lxml)"
        else
            print_warning "Some packages might not be installed correctly"
        fi
    fi
    
    print_success "Environment setup complete"
}

################################################################################
# Database Functions
################################################################################

check_database_status() {
    print_section "💾 Database Status"
    
    python3 << EOF
from pymongo import MongoClient
import sys

try:
    client = MongoClient('$MONGODB_URI', serverSelectionTimeoutMS=5000)
    db = client['$DATABASE_NAME']
    
    stats = {
        'filings': db.filings.count_documents({}),
        'facts': db.facts.count_documents({}),
        'contexts': db.contexts.count_documents({}),
        'units': db.units.count_documents({})
    }
    
    print(f"Database: $DATABASE_NAME")
    print(f"  📄 Filings:  {stats['filings']:>8,} documents")
    print(f"  📊 Facts:    {stats['facts']:>8,} documents")
    print(f"  📅 Contexts: {stats['contexts']:>8,} documents")
    print(f"  🔢 Units:    {stats['units']:>8,} documents")
    print(f"  ─────────────────────────────")
    print(f"  💾 Total:    {sum(stats.values()):>8,} documents")
    
    if stats['filings'] > 0:
        tickers = db.filings.distinct('ticker')
        print(f"\n🏢 Companies: {', '.join(sorted(tickers))}")
    
    client.close()
    sys.exit(0)
except Exception as e:
    print(f"✗ Database check failed: {e}")
    sys.exit(1)
EOF
    
    echo ""
}

clean_database() {
    print_section "🧹 Cleaning Database"
    
    python3 << EOF
from pymongo import MongoClient

client = MongoClient('$MONGODB_URI')
db = client['$DATABASE_NAME']

collections = ['filings', 'facts', 'contexts', 'units']
total_removed = 0

for coll in collections:
    count = db[coll].count_documents({})
    if count > 0:
        db[coll].drop()
        print(f"  ✓ Dropped {coll}: {count:,} documents")
        total_removed += count
    else:
        print(f"  • {coll}: already empty")

print(f"\n✓ Total removed: {total_removed:,} documents")
client.close()
EOF
    
    print_success "Database cleaned"
    echo ""
}

clean_cache() {
    print_section "🧹 Cleaning Cache"
    
    if [ -d "./cache" ]; then
        local file_count=$(find ./cache -type f 2>/dev/null | wc -l | tr -d ' ')
        local cache_size=$(du -sh ./cache 2>/dev/null | cut -f1)
        print_info "Cache: $cache_size ($file_count files)"
        rm -rf ./cache/*
        print_success "Cache cleaned"
    else
        print_info "Cache directory doesn't exist"
    fi
    echo ""
}

################################################################################
# Main Execution
################################################################################

run_wrapper() {
    local args=("$@")
    
    print_section "🚀 Launching Pipeline"
    
    print_info "Executing wrapper with arguments:"
    echo -e "${DIM}  python3 $WRAPPER_SCRIPT ${args[*]}${NC}"
    echo ""
    
    # Execute wrapper
    python3 "$WRAPPER_SCRIPT" "${args[@]}"
    
    return $?
}

################################################################################
# Main Function
################################################################################

main() {
    local wrapper_args=()
    local skip_setup=false
    local check_prereqs_only=false
    local clean_db=false
    local clean_cache_dir=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --setup-only)
                print_banner
                check_prerequisites || exit 1
                setup_environment
                print_box "✓ Setup Complete" "Environment ready" "Run pipeline with --demo or --ticker"
                exit 0
                ;;
            --check-prereqs)
                print_banner
                check_prerequisites
                exit $?
                ;;
            --clean)
                clean_db=true
                shift
                ;;
            --clean-cache)
                clean_cache_dir=true
                shift
                ;;
            --clean-all)
                clean_db=true
                clean_cache_dir=true
                shift
                ;;
            --help|-h)
                print_usage
                exit 0
                ;;
            --query-only)
                skip_setup=false
                wrapper_args+=("$1")
                shift
                ;;
            *)
                wrapper_args+=("$1")
                shift
                ;;
        esac
    done
    
    # Print banner
    print_banner
    
    echo -e "${CYAN}Started: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}Working Directory: $(pwd)${NC}\n"
    
    # Check prerequisites
    if ! check_prerequisites; then
        exit 1
    fi
    
    # Setup environment (unless query-only)
    if [ "$skip_setup" = false ]; then
        setup_environment
    fi
    
    # Database status check
    check_database_status
    
    # Clean operations
    if [ "$clean_db" = true ]; then
        clean_database
    fi
    
    if [ "$clean_cache_dir" = true ]; then
        clean_cache
    fi
    
    # Run wrapper
    if run_wrapper "${wrapper_args[@]}"; then
        print_section "🎉 Success"
        print_box "✓ Pipeline Complete" \
            "All operations finished successfully" \
            "" \
            "Next steps:" \
            "  • Run with --interactive to explore data" \
            "  • Use --demo-queries for automated queries" \
            "  • View data: mongosh $MONGODB_URI"
        exit 0
    else
        print_section "❌ Failed"
        print_error "Pipeline execution failed"
        print_info "Try running with --verbose for more details"
        exit 1
    fi
}

# Export environment variables
export MONGODB_URI DATABASE_NAME

# Run main
main "$@"