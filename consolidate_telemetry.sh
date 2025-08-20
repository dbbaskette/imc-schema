#!/bin/bash
# =============================================================================
# Telemetry Data Consolidation Script
# =============================================================================
# 
# This script consolidates small Parquet files into larger, more efficient files
# for better query performance and reduced storage overhead in HDFS.
#
# Usage Examples:
#   ./consolidate_telemetry.sh --date 2025-08-15 --dry-run
#   ./consolidate_telemetry.sh --date 2025-08-15 --target-size 256
#   ./consolidate_telemetry.sh --date 2025-08-15 --days-back 7
#
# =============================================================================

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/parquet_consolidator.py"
VENV_DIR="$SCRIPT_DIR/venv-consolidation"
DEFAULT_TARGET_SIZE=128
DEFAULT_DAYS_BACK=1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
🚀 Telemetry Data Consolidation Tool

DESCRIPTION:
    Consolidates small Parquet files in HDFS into larger, more efficient files
    for improved query performance and reduced small file overhead.

USAGE:
    $0 [OPTIONS]

OPTIONS:
    --date DATE             Date to consolidate (YYYY-MM-DD format) [REQUIRED]
    --target-size SIZE      Target consolidated file size in MB (default: $DEFAULT_TARGET_SIZE)
    --days-back N           Consolidate N days back from --date (default: $DEFAULT_DAYS_BACK)
    --dry-run              Show what would be done without making changes
    --verbose              Enable debug logging
    --help                 Show this help message

EXAMPLES:
    # Dry run for single date
    $0 --date 2025-08-15 --dry-run

    # Consolidate single date with 256MB target size
    $0 --date 2025-08-15 --target-size 256

    # Consolidate last 7 days
    $0 --date 2025-08-15 --days-back 7

    # Verbose consolidation
    $0 --date 2025-08-15 --verbose

REQUIREMENTS:
    - Python 3.7+ with pandas, pyarrow, hdfs3
    - HDFS client tools (hdfs command)
    - Access to HDFS cluster
    - Sufficient local disk space for temporary files

HDFS PATHS:
    Source: /insurance-megacorp/telemetry-data-v2/date=YYYY-MM-DD/
    Files:  telemetry-YYYYMMDD_HHMMSS-cf-X-writer-Y-*.parquet

SAFETY FEATURES:
    - Verifies consolidated file integrity before removing sources
    - Creates backups in temp directory during processing
    - Only removes source files after successful consolidation
    - Minimum file count threshold (5) before consolidation

EOF
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is required but not installed"
        return 1
    fi
    
    # Check HDFS client
    if ! command -v hdfs &> /dev/null; then
        log_error "HDFS client tools are required but not installed"
        return 1
    fi
    
    # Check Python script exists
    if [[ ! -f "$PYTHON_SCRIPT" ]]; then
        log_error "Python consolidation script not found: $PYTHON_SCRIPT"
        return 1
    fi
    
    # Check Python packages (in venv)
    if [[ -d "$VENV_DIR" ]]; then
        log_info "Activating Python virtual environment..."
        source "$VENV_DIR/bin/activate"
    else
        log_warning "Virtual environment not found at $VENV_DIR"
        log_info "Please run ./setup_consolidation_env.sh first"
    fi

    if ! python3 -c "import pandas, pyarrow" 2>/dev/null; then
        log_warning "Some Python packages may be missing (pandas, pyarrow, hdfs3)"
        log_info "Install with: ./setup_consolidation_env.sh"
    fi
    
    log_success "Dependencies check passed"
    return 0
}

validate_date() {
    local date_str="$1"
    
    # Check date format
    if [[ ! "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log_error "Invalid date format. Use YYYY-MM-DD"
        return 1
    fi
    
    # Check if date is valid (macOS compatible)
    local year month day
    IFS='-' read -r year month day <<< "$date_str"
    
    # Basic validation
    if [[ $year -lt 1900 || $year -gt 2100 ]]; then
        log_error "Invalid year in date: $date_str"
        return 1
    fi
    
    if [[ 10#$month -lt 1 || 10#$month -gt 12 ]]; then
        log_error "Invalid month in date: $date_str"
        return 1
    fi
    
    if [[ 10#$day -lt 1 || 10#$day -gt 31 ]]; then
        log_error "Invalid day in date: $date_str"
        return 1
    fi
    
    return 0
}

get_hdfs_file_count() {
    local date="$1"
    local hdfs_path="/insurance-megacorp/telemetry-data-v2/date=$date"
    
    # Count telemetry files for the date
    local count
    count=$(hdfs dfs -ls "$hdfs_path" 2>/dev/null | grep -c "telemetry-.*\.parquet" || echo "0")
    echo "$count"
}

show_consolidation_preview() {
    local date="$1"
    local hdfs_path="/insurance-megacorp/telemetry-data-v2/date=$date"
    
    log_info "Preview for date $date:"
    
    if hdfs dfs -test -d "$hdfs_path" 2>/dev/null; then
        local file_count
        file_count=$(get_hdfs_file_count "$date")
        
        if [[ "$file_count" -gt 0 ]]; then
            log_info "  📁 Found $file_count Parquet files"
            
            # Show total size
            local total_size_bytes
            local du_output
            du_output=$(hdfs dfs -du -s "$hdfs_path" 2>&1)
            total_size_bytes=$(echo "$du_output" | grep '^[0-9]' | tail -1 | awk '{print $1}' || echo "0")
            log_info "  📊 Total size: $(numfmt --to=iec "$total_size_bytes")"
            
            # Show sample files
            log_info "  📄 Sample files:"
            while read -r line; do
                local filename
                filename=$(echo "$line" | awk '{print $NF}' | xargs basename)
                local size
                size=$(echo "$line" | awk '{print $5}')
                log_info "    • $filename ($(numfmt --to=iec "$size"))"
            done < <(hdfs dfs -ls "$hdfs_path" 2>/dev/null | grep "telemetry-.*\\.parquet" | head -3)
            
            if [[ "$file_count" -gt 3 ]]; then
                log_info "    ... and $((file_count - 3)) more files"
            fi
        else
            log_warning "  No telemetry files found for date $date"
        fi
    else
        log_warning "  Directory does not exist: $hdfs_path"
    fi
    
    echo
}

main() {
    # Parse command line arguments
    local date=""
    local target_size="$DEFAULT_TARGET_SIZE"
    local days_back=""
    local dry_run=false
    local verbose=false
    
    # Set dry_run to true if --dry-run is present
    for arg in "$@"; do
        if [[ "$arg" == "--dry-run" ]]; then
            dry_run=true
            break
        fi
    done
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --date)
                date="$2"
                shift 2
                ;;
            --target-size)
                target_size="$2"
                shift 2
                ;;
            --days-back)
                days_back="$2"
                shift 2
                ;;
            --dry-run)
                # This is now handled by the loop above, but we keep it to consume the argument
                shift
                ;;
            --verbose)
                verbose=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Validate required arguments
    if [[ -z "$date" ]]; then
        log_error "Date is required. Use --date YYYY-MM-DD"
        show_help
        exit 1
    fi
    
    if ! validate_date "$date"; then
        exit 1
    fi
    
    # Check dependencies
    if ! check_dependencies; then
        exit 1
    fi
    
    # Show header
    echo "🚀 Insurance MegaCorp - Telemetry Data Consolidation"
    echo "=================================================="
    echo
    
    # Show what will be processed
    if [[ -n "$days_back" ]]; then
        log_info "Will process $((days_back + 1)) days ending on $date"
        echo
        
        # Show preview for each date
        for ((i=days_back; i>=0; i--)); do
            local process_date
            # macOS compatible date calculation
            if command -v gdate &> /dev/null; then
                # GNU date (if installed via brew install coreutils)
                process_date=$(gdate -d "$date - $i days" +%Y-%m-%d)
            else
                # BSD date (macOS default) - using Python as fallback for date math
                process_date=$(python3 -c "
import datetime
base = datetime.datetime.strptime('$date', '%Y-%m-%d')
target = base - datetime.timedelta(days=$i)
print(target.strftime('%Y-%m-%d'))
")
            fi
            show_consolidation_preview "$process_date"
        done
    else
        log_info "Will process single date: $date"
        echo
        show_consolidation_preview "$date"
    fi
    
    # Build Python command
    local python_args=("--date" "$date" "--target-size" "$target_size")
    
    if [[ -n "$days_back" ]]; then
        python_args+=("--days-back" "$days_back")
    fi
    
    if [[ "$dry_run" == true ]]; then
        python_args+=("--dry-run")
        log_info "🔍 DRY RUN MODE - No changes will be made"
        echo
    fi
    
    if [[ "$verbose" == true ]]; then
        python_args+=("--verbose")
    fi
    
    # Execute consolidation
    log_info "Starting consolidation process..."
    
    if [[ "$dry_run" == true ]]; then
        log_info "🔍 DRY RUN MODE - No changes will be made"
    fi
    
    echo
    
    if python3 "$PYTHON_SCRIPT" "${python_args[@]}"; then
        echo
        if [[ "$dry_run" == true ]]; then
            log_success "Dry run completed successfully"
            log_info "Run without --dry-run to perform actual consolidation"
        else
            log_success "Consolidation completed successfully"
        fi
    else
        echo
        log_error "Consolidation failed"
        exit 1
    fi
}

# Execute main function with all arguments
main "$@"
