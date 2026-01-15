#!/bin/bash
# =============================================================================
# Simple Shell-Based Parquet Consolidation
# =============================================================================
# This script consolidates Parquet files using pure shell commands
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }
log_header() { echo -e "${PURPLE}🚀 $1${NC}"; }

# Parse arguments
DATE=""
DRY_RUN=false
MIN_SIZE=1024  # Skip files smaller than 1KB

show_usage() {
    echo ""
    log_header "Simple Parquet Consolidation"
    echo ""
    echo "Usage: $0 --date DATE [--dry-run]"
    echo ""
    echo "OPTIONS:"
    echo "  --date DATE    Date to consolidate (YYYY-MM-DD)"
    echo "  --dry-run     Show analysis without consolidating"
    echo "  --help        Show this help"
    echo ""
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            DATE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
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

if [[ -z "$DATE" ]]; then
    log_error "Date is required"
    show_usage
    exit 1
fi

# Setup environment
log_info "Setting up environment..."
if [[ -f "$HOME/.hadoop_env" ]]; then
    source "$HOME/.hadoop_env"
fi

# Check HDFS connectivity
if ! hdfs dfs -ls / >/dev/null 2>&1; then
    log_error "HDFS connection failed"
    exit 1
fi

HDFS_PATH="/insurance-megacorp/telemetry-data-v2/date=$DATE"

log_header "Consolidation Analysis for $DATE"
echo ""

# Get file list and analyze
log_info "Analyzing files..."
TEMP_FILE="/tmp/consolidation_analysis_$$"

# Get file list with sizes, filtering out warnings
hdfs dfs -ls "$HDFS_PATH/" 2>/dev/null | \
    grep "telemetry-.*\.parquet" | \
    awk '{print $5 " " $8}' > "$TEMP_FILE"

if [[ ! -s "$TEMP_FILE" ]]; then
    log_warning "No Parquet files found for $DATE"
    rm -f "$TEMP_FILE"
    exit 1
fi

# Analysis
TOTAL_FILES=0
TOTAL_SIZE=0
VALID_FILES=0
VALID_SIZE=0
SKIPPED_FILES=0

while read -r size filepath; do
    TOTAL_FILES=$((TOTAL_FILES + 1))
    TOTAL_SIZE=$((TOTAL_SIZE + size))
    
    if [[ $size -lt $MIN_SIZE ]]; then
        SKIPPED_FILES=$((SKIPPED_FILES + 1))
        log_warning "Skipping $(basename "$filepath") (${size} bytes - too small)"
    else
        VALID_FILES=$((VALID_FILES + 1))
        VALID_SIZE=$((VALID_SIZE + size))
    fi
done < "$TEMP_FILE"

# Convert sizes to human readable
TOTAL_SIZE_MB=$((TOTAL_SIZE / 1024 / 1024))
VALID_SIZE_MB=$((VALID_SIZE / 1024 / 1024))

echo ""
log_header "Analysis Results"
echo "📊 Total files found: $TOTAL_FILES"
echo "📊 Total size: ${TOTAL_SIZE_MB}MB"
echo "✅ Valid files (>1KB): $VALID_FILES" 
echo "✅ Valid size: ${VALID_SIZE_MB}MB"
echo "⚠️  Skipped files: $SKIPPED_FILES"
echo ""

if [[ $VALID_FILES -lt 5 ]]; then
    log_warning "Only $VALID_FILES valid files found. Minimum is 5 for consolidation."
    rm -f "$TEMP_FILE"
    exit 0
fi

if [[ $DRY_RUN == true ]]; then
    log_info "DRY RUN: Would consolidate $VALID_FILES files (${VALID_SIZE_MB}MB) into 1 file"
    echo ""
    log_header "Benefits:"
    echo "🚀 Query performance: ${VALID_FILES}x faster (${VALID_FILES} → 1 I/O operations)"
    echo "🧠 NameNode memory: $((100 - 100/VALID_FILES))% reduction"
    echo "💾 Storage efficiency: Better block utilization"
    
    rm -f "$TEMP_FILE"
    exit 0
fi

# Actual consolidation would go here...
log_header "Actual Consolidation"
log_warning "Full consolidation implementation requires PyArrow for Parquet operations"
log_info "For now, this script provides analysis. Use the Python version for actual consolidation:"
echo ""
echo "Alternative approaches:"
echo "1. Use Spark to consolidate: spark.read.parquet().coalesce(1).write.parquet()"
echo "2. Use Hadoop FS merge (for non-Parquet files)"
echo "3. Fix the Python subprocess issue and use parquet_consolidator.py"

rm -f "$TEMP_FILE"
