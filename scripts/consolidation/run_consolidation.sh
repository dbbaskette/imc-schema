#!/bin/bash
# =============================================================================
# Insurance MegaCorp - Parquet Consolidation Runner
# =============================================================================
# This script handles all the setup and running of the Python consolidation tool
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print colored output
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

log_header() {
    echo -e "${PURPLE}🚀 $1${NC}"
}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Show usage
show_usage() {
    echo ""
    log_header "Parquet Consolidation Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  --date DATE         Date to consolidate (format: YYYY-MM-DD)"
    echo "  --dry-run          Show what would be done without actually doing it"
    echo "  --target-size SIZE Target file size (e.g., 128MB, 256MB)"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --date 2025-08-15 --dry-run"
    echo "  $0 --date 2025-08-14"
    echo "  $0 --date 2025-08-15 --target-size 256MB"
    echo ""
}

# Parse arguments
DATE=""
DRY_RUN=""
TARGET_SIZE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            DATE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --target-size)
            TARGET_SIZE="--target-size $2"
            shift 2
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$DATE" ]]; then
    log_error "Date is required. Use --date YYYY-MM-DD"
    show_usage
    exit 1
fi

# Validate date format
if [[ ! "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log_error "Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

log_header "Starting Parquet Consolidation for $DATE"
echo ""

# Step 1: Check if config.env exists
log_info "Checking configuration..."
if [[ ! -f "config.env" ]]; then
    log_error "config.env not found. Please copy config.env.example to config.env and update with your settings."
    exit 1
fi
log_success "Configuration found"

# Step 2: Setup Hadoop environment
log_info "Setting up Hadoop environment..."
if [[ -f "$HOME/.hadoop_env" ]]; then
    source "$HOME/.hadoop_env"
    log_success "Hadoop environment loaded"
else
    log_warning "Hadoop environment not found. Running setup..."
    if [[ -f "setup_hadoop_client.sh" ]]; then
        ./setup_hadoop_client.sh
        source "$HOME/.hadoop_env"
        log_success "Hadoop environment setup completed"
    else
        log_error "setup_hadoop_client.sh not found"
        exit 1
    fi
fi

# Step 3: Check Python virtual environment
log_info "Setting up Python environment..."
if [[ ! -d "venv-consolidation" ]]; then
    log_warning "Python virtual environment not found. Creating..."
    if [[ -f "setup_consolidation_env.sh" ]]; then
        ./setup_consolidation_env.sh
        log_success "Python environment created"
    else
        log_error "setup_consolidation_env.sh not found"
        exit 1
    fi
fi

# Activate virtual environment
source venv-consolidation/bin/activate
log_success "Python environment activated"

# Step 4: Test HDFS connectivity
log_info "Testing HDFS connectivity..."
if hdfs dfs -ls / >/dev/null 2>&1; then
    log_success "HDFS connection working"
else
    log_error "HDFS connection failed. Please check your configuration."
    exit 1
fi

# Step 5: Show what we're about to do
echo ""
log_header "Consolidation Parameters"
echo "📅 Date: $DATE"
echo "🎯 Mode: $(if [[ -n "$DRY_RUN" ]]; then echo "DRY RUN (no changes)"; else echo "ACTUAL CONSOLIDATION"; fi)"
echo "📦 Target Size: $(if [[ -n "$TARGET_SIZE" ]]; then echo "${TARGET_SIZE#--target-size }"; else echo "128MB (default)"; fi)"
echo ""

# Step 6: Quick file preview
log_info "Checking available files for $DATE..."
FILE_COUNT=$(hdfs dfs -ls "/insurance-megacorp/telemetry-data-v2/date=$DATE/" 2>/dev/null | grep "telemetry-" | wc -l || echo "0")
if [[ "$FILE_COUNT" -eq 0 ]]; then
    log_warning "No telemetry files found for date $DATE"
    log_info "Available dates:"
    hdfs dfs -ls /insurance-megacorp/telemetry-data-v2/ 2>/dev/null | grep "date=" | awk '{print $8}' | sed 's/.*date=/  - /' | head -10
    exit 1
else
    log_success "Found $FILE_COUNT files for $DATE"
fi

# Step 7: Run the consolidation
echo ""
log_header "Running Consolidation"

# Build the command
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON_CMD="python3 $REPO_ROOT/python/parquet_consolidator.py --date $DATE $DRY_RUN $TARGET_SIZE"

log_info "Executing: $PYTHON_CMD"
echo ""

# Run with better error handling
if eval "$PYTHON_CMD"; then
    echo ""
    log_success "Consolidation completed successfully!"
    
    if [[ -z "$DRY_RUN" ]]; then
        log_info "Verifying results..."
        CONSOLIDATED_COUNT=$(hdfs dfs -ls "/insurance-megacorp/telemetry-data-v2/date=$DATE/" 2>/dev/null | grep "consolidated" | wc -l || echo "0")
        if [[ "$CONSOLIDATED_COUNT" -gt 0 ]]; then
            log_success "Found $CONSOLIDATED_COUNT consolidated file(s)"
        else
            log_warning "No consolidated files found. Check the logs above for details."
        fi
    fi
else
    echo ""
    log_error "Consolidation failed. Check the error messages above."
    exit 1
fi

echo ""
log_header "Consolidation Complete! 🎉"
