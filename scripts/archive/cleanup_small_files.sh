#!/bin/bash
# =============================================================================
# Insurance MegaCorp - Small File Cleanup Script
# =============================================================================
# This script scans the entire telemetry-data-v2 directory and removes files
# smaller than 10KB to prevent consolidation issues
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

# Configuration
TELEMETRY_BASE="/insurance-megacorp/telemetry-data-v2"
MIN_SIZE_KB=10
DRY_RUN=""

# Show usage
show_usage() {
    echo ""
    log_header "Small File Cleanup Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  --dry-run          Show what would be deleted without actually deleting"
    echo "  --min-size SIZE    Minimum file size in KB (default: 10)"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --dry-run"
    echo "  $0 --min-size 5"
    echo "  $0"
    echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --min-size)
            MIN_SIZE_KB="$2"
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

log_header "Starting Small File Cleanup"
echo ""
log_info "Configuration:"
echo "📁 Base Directory: $TELEMETRY_BASE"
echo "📏 Minimum Size: ${MIN_SIZE_KB}KB"
echo "🎯 Mode: $(if [[ -n "$DRY_RUN" ]]; then echo "DRY RUN (no changes)"; else echo "ACTUAL DELETION"; fi)"
echo ""

# Check if config.env exists and load Hadoop environment
if [[ -f "config.env" ]]; then
    log_info "Loading configuration..."
    source config.env
    log_success "Configuration loaded"
else
    log_warning "config.env not found, using default settings"
fi

# Setup Hadoop environment
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

# Test HDFS connectivity
log_info "Testing HDFS connectivity..."
if hdfs dfs -ls / >/dev/null 2>&1; then
    log_success "HDFS connection working"
else
    log_error "HDFS connection failed. Please check your configuration."
    exit 1
fi

# Check if base directory exists
log_info "Checking base directory..."
if hdfs dfs -test -d "$TELEMETRY_BASE" 2>/dev/null; then
    log_success "Base directory exists"
else
    log_error "Base directory $TELEMETRY_BASE not found"
    exit 1
fi

echo ""
log_header "Scanning for Small Files"
echo ""

# Function to scan and process files
scan_and_cleanup() {
    local total_files=0
    local small_files=0
    local total_size_mb=0
    local deleted_size_mb=0
    
    log_info "Scanning directory: $TELEMETRY_BASE"
    
    # Get all files recursively and process them
    while IFS= read -r line; do
        if [[ -z "$line" ]]; then
            continue
        fi
        
        # Parse the HDFS ls output
        # Format: -rw-r--r--   3 hdfs supergroup   499094 2025-08-18 21:00 /path/to/file
        if [[ "$line" =~ ^- ]]; then
            total_files=$((total_files + 1))
            
            # Extract file size and path
            read -r permissions links owner group size_bytes date time path <<< "$line"
            
            # Convert bytes to KB
            size_kb=$((size_bytes / 1024))
            
            # Check if file is smaller than minimum
            if [[ $size_kb -lt $MIN_SIZE_KB ]]; then
                small_files=$((small_files + 1))
                deleted_size_mb=$((deleted_size_mb + size_bytes / 1024 / 1024))
                
                if [[ -n "$DRY_RUN" ]]; then
                    log_warning "Would delete: $path (${size_kb}KB)"
                else
                    log_info "Deleting: $path (${size_kb}KB)"
                    if HADOOP_USER_NAME=hdfs hdfs dfs -rm "$path" >/dev/null 2>&1; then
                        log_success "Deleted: $path"
                    else
                        log_error "Failed to delete: $path"
                    fi
                fi
            fi
            
            # Add to total size
            total_size_mb=$((total_size_mb + size_bytes / 1024 / 1024))
        fi
    done < <(hdfs dfs -ls -R "$TELEMETRY_BASE" 2>/dev/null | grep -E "^-")
    
    echo ""
    log_header "Scan Results"
    echo "📊 Total files scanned: $total_files"
    echo "📏 Files smaller than ${MIN_SIZE_KB}KB: $small_files"
    echo "💾 Total data size: ${total_size_mb}MB"
    echo "🗑️  Size of files to be deleted: ${deleted_size_mb}MB"
    
    if [[ -n "$DRY_RUN" ]]; then
        echo ""
        log_warning "DRY RUN: No files were actually deleted"
        log_info "Run without --dry-run to perform actual deletion"
    else
        echo ""
        log_success "Cleanup completed!"
    fi
}

# Run the scan and cleanup
scan_and_cleanup

echo ""
log_header "Cleanup Complete! 🎉"
