#!/bin/bash
# =============================================================================
# Preview Consolidation Script (No Python Dependencies)
# =============================================================================
# 
# This script previews what would be consolidated without requiring Python
# packages. Useful for understanding your current file structure.
#
# =============================================================================

set -euo pipefail

# Configuration
HDFS_BASE_PATH="/insurance-megacorp/telemetry-data-v2"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

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

# Function to convert bytes to human readable
human_readable() {
    local bytes=$1
    if [[ $bytes -lt 1024 ]]; then
        echo "${bytes}B"
    elif [[ $bytes -lt 1048576 ]]; then
        echo "$((bytes/1024))KB"
    elif [[ $bytes -lt 1073741824 ]]; then
        echo "$((bytes/1048576))MB"
    else
        echo "$((bytes/1073741824))GB"
    fi
}

# Function to analyze a specific date
analyze_date() {
    local date="$1"
    local hdfs_path="$HDFS_BASE_PATH/date=$date"
    
    echo -e "${CYAN}📅 Analyzing Date: $date${NC}"
    echo "----------------------------------------"
    
    # Check if directory exists
    if ! hdfs dfs -test -d "$hdfs_path" 2>/dev/null; then
        log_warning "Directory does not exist: $hdfs_path"
        return 0
    fi
    
    # Get file listing
    local temp_file=$(mktemp)
    if ! hdfs dfs -ls "$hdfs_path" 2>/dev/null | grep "telemetry-.*\.parquet" > "$temp_file"; then
        log_warning "No telemetry Parquet files found in $hdfs_path"
        rm -f "$temp_file"
        return 0
    fi
    
    # Count files and calculate sizes
    local file_count=0
    local total_bytes=0
    local min_size=999999999
    local max_size=0
    local sizes=()
    
    while IFS= read -r line; do
        # Extract size (5th column) and filename
        local size=$(echo "$line" | awk '{print $5}')
        local filename=$(echo "$line" | awk '{print $NF}' | xargs basename)
        
        file_count=$((file_count + 1))
        total_bytes=$((total_bytes + size))
        sizes+=($size)
        
        if [[ $size -lt $min_size ]]; then
            min_size=$size
        fi
        if [[ $size -gt $max_size ]]; then
            max_size=$size
        fi
        
        # Show first few files as examples
        if [[ $file_count -le 5 ]]; then
            printf "  📄 %-50s %8s\n" "$filename" "$(human_readable $size)"
        fi
    done < "$temp_file"
    
    if [[ $file_count -gt 5 ]]; then
        echo "  📄 ... and $((file_count - 5)) more files"
    fi
    
    echo
    
    # Calculate statistics
    local avg_size=$((total_bytes / file_count))
    
    # Show summary
    echo -e "${GREEN}📊 Summary:${NC}"
    printf "  📁 Total Files:     %d\n" "$file_count"
    printf "  📏 Total Size:      %s\n" "$(human_readable $total_bytes)"
    printf "  📐 Average Size:    %s\n" "$(human_readable $avg_size)"
    printf "  📉 Smallest File:   %s\n" "$(human_readable $min_size)"
    printf "  📈 Largest File:    %s\n" "$(human_readable $max_size)"
    echo
    
    # Consolidation analysis
    echo -e "${YELLOW}⚡ Consolidation Impact:${NC}"
    if [[ $file_count -ge 5 ]]; then
        local target_size_mb=128
        local target_bytes=$((target_size_mb * 1024 * 1024))
        local estimated_files=$(( (total_bytes + target_bytes - 1) / target_bytes ))
        [[ $estimated_files -eq 0 ]] && estimated_files=1
        
        local reduction_percent=$(( (file_count - estimated_files) * 100 / file_count ))
        
        printf "  🎯 Target File Size: %dMB\n" "$target_size_mb"
        printf "  📦 Files After:     %d\n" "$estimated_files"
        printf "  📉 Reduction:       %d%% (%d → %d files)\n" "$reduction_percent" "$file_count" "$estimated_files"
        
        if [[ $file_count -gt 10 ]]; then
            log_success "✅ Excellent candidate for consolidation!"
        elif [[ $file_count -gt 5 ]]; then
            log_info "✅ Good candidate for consolidation"
        else
            log_info "✅ Would consolidate (meets minimum threshold)"
        fi
    else
        log_warning "❌ Below minimum threshold (need ≥5 files, found $file_count)"
    fi
    
    echo
    
    # Query performance estimate
    echo -e "${BLUE}🚀 Performance Impact:${NC}"
    printf "  📊 Current I/O Ops: %d (one per file)\n" "$file_count"
    printf "  ⚡ After Consol:    1-2 (consolidated files)\n"
    if [[ $file_count -gt 1 ]]; then
        local speedup=$(( file_count / 2 ))
        printf "  🏃 Query Speedup:   ~%dx faster\n" "$speedup"
    fi
    
    rm -f "$temp_file"
    echo
    echo "========================================="
    echo
}

# Function to get available dates
get_available_dates() {
    local dates=()
    
    log_info "Scanning for available dates in $HDFS_BASE_PATH..."
    
    # Get date directories
    if hdfs dfs -ls "$HDFS_BASE_PATH" 2>/dev/null | grep "date=" | head -10; then
        while IFS= read -r line; do
            local dir_name=$(echo "$line" | awk '{print $NF}' | xargs basename)
            if [[ $dir_name =~ date=([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
                dates+=("${BASH_REMATCH[1]}")
            fi
        done < <(hdfs dfs -ls "$HDFS_BASE_PATH" 2>/dev/null | grep "date=")
    fi
    
    echo "${dates[@]}"
}

# Main function
main() {
    local target_date=""
    local analyze_all=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --date)
                target_date="$2"
                shift 2
                ;;
            --all)
                analyze_all=true
                shift
                ;;
            --help)
                echo "🔍 Consolidation Preview Tool"
                echo ""
                echo "USAGE:"
                echo "  $0 --date YYYY-MM-DD    Analyze specific date"
                echo "  $0 --all                Analyze all available dates"
                echo "  $0 --help               Show this help"
                echo ""
                echo "EXAMPLES:"
                echo "  $0 --date 2025-08-15"
                echo "  $0 --all"
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    echo "🔍 Insurance MegaCorp - Consolidation Preview"
    echo "============================================="
    echo
    
    if [[ "$analyze_all" == true ]]; then
        # Analyze all available dates
        local available_dates
        available_dates=($(get_available_dates))
        
        if [[ ${#available_dates[@]} -eq 0 ]]; then
            log_warning "No date directories found in $HDFS_BASE_PATH"
            exit 1
        fi
        
        log_info "Found ${#available_dates[@]} date(s) to analyze"
        echo
        
        for date in "${available_dates[@]}"; do
            analyze_date "$date"
        done
        
    elif [[ -n "$target_date" ]]; then
        # Analyze specific date
        if [[ ! "$target_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
            log_error "Invalid date format. Use YYYY-MM-DD"
            exit 1
        fi
        
        analyze_date "$target_date"
        
    else
        log_error "Please specify --date YYYY-MM-DD or --all"
        echo "Use --help for usage information"
        exit 1
    fi
}

# Run main function
main "$@"
