#!/bin/bash
# =============================================================================
# Telemetry Data Cleanup Script
# =============================================================================
# Scans telemetry_data_v2 directory and removes files ≤10KB
# Generates detailed report of cleanup actions
# =============================================================================

set -euo pipefail

# Load configuration (find repo root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/config.env"

# Configuration
SIZE_THRESHOLD_KB=1  # Only delete files ≤1KB (corrupted/empty files)
SIZE_THRESHOLD_BYTES=$((SIZE_THRESHOLD_KB * 1024))
TELEMETRY_BASE_PATH="${1:-hdfs://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}/insurance-megacorp/telemetry-data-v2}"
REPORT_DIR="${LOG_DIR:-./logs}"
REPORT_FILE="$REPORT_DIR/telemetry_cleanup_$(date +%Y%m%d_%H%M%S).log"

# Create report directory
mkdir -p "$REPORT_DIR"

# Initialize counters
TOTAL_FILES=0
SMALL_FILES=0
DELETED_FILES=0
TOTAL_SIZE_BEFORE=0
TOTAL_SIZE_AFTER=0
FREED_SPACE=0

# Arrays to store file information
declare -a SMALL_FILE_LIST=()
declare -a DELETED_FILE_LIST=()
declare -a ERROR_LIST=()

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$REPORT_FILE"
}

# Human readable size function
human_readable() {
    local bytes=$1
    # Check if bytes is a valid number
    if ! [[ "$bytes" =~ ^[0-9]+$ ]]; then
        echo "0B"
        return
    fi
    
    if [ $bytes -ge 1073741824 ]; then
        echo "$(echo "$bytes" | awk '{printf "%.2f", $1/1073741824}')GB"
    elif [ $bytes -ge 1048576 ]; then
        echo "$(echo "$bytes" | awk '{printf "%.2f", $1/1048576}')MB"
    elif [ $bytes -ge 1024 ]; then
        echo "$(echo "$bytes" | awk '{printf "%.2f", $1/1024}')KB"
    else
        echo "${bytes}B"
    fi
}

# Error handling
handle_error() {
    log "ERROR: Script failed at line $1"
    log "ERROR: Command: $BASH_COMMAND"
    exit 1
}

trap 'handle_error $LINENO' ERR

# =============================================================================
# Main Execution
# =============================================================================

log "=========================================="
log "TELEMETRY DATA CLEANUP REPORT"
log "=========================================="
log "Start Time: $(date)"
log "Target Path: $TELEMETRY_BASE_PATH"
log "Size Threshold: ${SIZE_THRESHOLD_KB}KB (${SIZE_THRESHOLD_BYTES} bytes)"
log "Report File: $REPORT_FILE"
log ""

# Check if we're working with HDFS or local filesystem
if [[ "$TELEMETRY_BASE_PATH" == hdfs://* ]]; then
    log "🔍 Scanning HDFS directory: $TELEMETRY_BASE_PATH"
    
    # Use HDFS commands for scanning
    if ! command -v hdfs >/dev/null 2>&1; then
        log "ERROR: hdfs command not found. Please ensure Hadoop client is installed."
        exit 1
    fi
    
    # Get list of all files recursively
    log "📊 Analyzing file sizes..."
    
    # Create temporary file for HDFS file listing
    TEMP_FILE=$(mktemp)
    
    # Get detailed file listing from HDFS
    HADOOP_USER_NAME=hdfs hdfs dfs -ls -R "$TELEMETRY_BASE_PATH" | grep "^-" > "$TEMP_FILE" || {
        log "ERROR: Failed to access HDFS path: $TELEMETRY_BASE_PATH"
        rm -f "$TEMP_FILE"
        exit 1
    }
    
    # Process each file
    while IFS= read -r line; do
        # Parse HDFS ls output: permissions replication userid groupid filesize date time filename
        # Example: -rw-r--r--   3 hdfs supergroup      12345 2024-01-15 10:30 /path/to/file.parquet
        
        if [[ -n "$line" ]]; then
            # Extract file size (6th field) and filename (last field)
            FILE_SIZE=$(echo "$line" | awk '{print $5}')
            FILE_PATH=$(echo "$line" | awk '{print $NF}')
            
            TOTAL_FILES=$((TOTAL_FILES + 1))
            TOTAL_SIZE_BEFORE=$((TOTAL_SIZE_BEFORE + FILE_SIZE))
            
            if [ "$FILE_SIZE" -le "$SIZE_THRESHOLD_BYTES" ]; then
                SMALL_FILES=$((SMALL_FILES + 1))
                SMALL_FILE_LIST+=("$FILE_PATH ($(human_readable $FILE_SIZE))")
                
                log "🗑️  Deleting small file: $FILE_PATH ($(human_readable $FILE_SIZE))"
                
                # Delete the file from HDFS
                if HADOOP_USER_NAME=hdfs hdfs dfs -rm "$FILE_PATH" >/dev/null 2>&1; then
                    DELETED_FILES=$((DELETED_FILES + 1))
                    FREED_SPACE=$((FREED_SPACE + FILE_SIZE))
                    DELETED_FILE_LIST+=("$FILE_PATH ($(human_readable $FILE_SIZE))")
                else
                    ERROR_LIST+=("Failed to delete: $FILE_PATH")
                    log "⚠️  WARNING: Failed to delete $FILE_PATH"
                fi
            else
                TOTAL_SIZE_AFTER=$((TOTAL_SIZE_AFTER + FILE_SIZE))
            fi
        fi
    done < "$TEMP_FILE"
    
    rm -f "$TEMP_FILE"
    
else
    # Local filesystem scanning
    log "🔍 Scanning local directory: $TELEMETRY_BASE_PATH"
    
    if [ ! -d "$TELEMETRY_BASE_PATH" ]; then
        log "ERROR: Directory does not exist: $TELEMETRY_BASE_PATH"
        exit 1
    fi
    
    log "📊 Analyzing file sizes..."
    
    # Find all files recursively
    while IFS= read -r -d '' file; do
        if [ -f "$file" ]; then
            FILE_SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
            TOTAL_FILES=$((TOTAL_FILES + 1))
            TOTAL_SIZE_BEFORE=$((TOTAL_SIZE_BEFORE + FILE_SIZE))
            
            if [ "$FILE_SIZE" -le "$SIZE_THRESHOLD_BYTES" ]; then
                SMALL_FILES=$((SMALL_FILES + 1))
                SMALL_FILE_LIST+=("$file ($(human_readable $FILE_SIZE))")
                
                log "🗑️  Deleting small file: $file ($(human_readable $FILE_SIZE))"
                
                # Delete the file
                if rm "$file" 2>/dev/null; then
                    DELETED_FILES=$((DELETED_FILES + 1))
                    FREED_SPACE=$((FREED_SPACE + FILE_SIZE))
                    DELETED_FILE_LIST+=("$file ($(human_readable $FILE_SIZE))")
                else
                    ERROR_LIST+=("Failed to delete: $file")
                    log "⚠️  WARNING: Failed to delete $file"
                fi
            else
                TOTAL_SIZE_AFTER=$((TOTAL_SIZE_AFTER + FILE_SIZE))
            fi
        fi
    done < <(find "$TELEMETRY_BASE_PATH" -type f -print0)
fi

# =============================================================================
# Generate Detailed Report
# =============================================================================

log ""
log "=========================================="
log "CLEANUP SUMMARY"
log "=========================================="
log "📁 Total Files Scanned: $TOTAL_FILES"
log "📏 Files ≤${SIZE_THRESHOLD_KB}KB: $SMALL_FILES"
log "🗑️  Files Successfully Deleted: $DELETED_FILES"
log "❌ Deletion Errors: ${#ERROR_LIST[@]}"
log ""
log "💾 Storage Summary:"
log "   • Total Size Before: $(human_readable $TOTAL_SIZE_BEFORE)"
log "   • Total Size After: $(human_readable $TOTAL_SIZE_AFTER)"
log "   • Space Freed: $(human_readable $FREED_SPACE)"
log "   • Space Reduction: $(echo "scale=2; $FREED_SPACE * 100 / $TOTAL_SIZE_BEFORE" | bc)%"
log ""

# Detailed file listings
if [ ${#DELETED_FILE_LIST[@]} -gt 0 ]; then
    log "=========================================="
    log "DELETED FILES (${#DELETED_FILE_LIST[@]} total)"
    log "=========================================="
    for file in "${DELETED_FILE_LIST[@]}"; do
        log "✅ $file"
    done
    log ""
fi

if [ ${#ERROR_LIST[@]} -gt 0 ]; then
    log "=========================================="
    log "ERRORS (${#ERROR_LIST[@]} total)"
    log "=========================================="
    for error in "${ERROR_LIST[@]}"; do
        log "❌ $error"
    done
    log ""
fi

# Small files that remain (should be none if all deletions succeeded)
REMAINING_SMALL=$((SMALL_FILES - DELETED_FILES))
if [ $REMAINING_SMALL -gt 0 ]; then
    log "=========================================="
    log "SMALL FILES REMAINING ($REMAINING_SMALL total)"
    log "=========================================="
    for ((i=DELETED_FILES; i<${#SMALL_FILE_LIST[@]}; i++)); do
        log "⚠️  ${SMALL_FILE_LIST[$i]}"
    done
    log ""
fi

# Directory structure analysis
log "=========================================="
log "DIRECTORY ANALYSIS"
log "=========================================="

if [[ "$TELEMETRY_BASE_PATH" == hdfs://* ]]; then
    # HDFS directory analysis
    log "📂 Directory structure (HDFS):"
    HADOOP_USER_NAME=hdfs hdfs dfs -du -s "$TELEMETRY_BASE_PATH"/* 2>/dev/null | while read size path; do
        log "   $(human_readable $size) - $(basename "$path")"
    done || log "   Unable to analyze directory structure"
else
    # Local directory analysis
    log "📂 Directory structure (Local):"
    du -h -d 1 "$TELEMETRY_BASE_PATH" 2>/dev/null | sort -hr | while read size path; do
        log "   $size - $(basename "$path")"
    done || log "   Unable to analyze directory structure"
fi

log ""
log "=========================================="
log "RECOMMENDATIONS"
log "=========================================="

if [ $FREED_SPACE -gt 0 ]; then
    log "✅ Successfully freed $(human_readable $FREED_SPACE) of storage space"
fi

if [ $SMALL_FILES -gt 0 ]; then
    log "💡 Consider investigating why small files are being generated:"
    log "   • Check telemetry data ingestion process"
    log "   • Verify data partitioning strategy"
    log "   • Review file size thresholds in data pipeline"
fi

if [ ${#ERROR_LIST[@]} -gt 0 ]; then
    log "⚠️  Some files could not be deleted - check permissions"
fi

log ""
log "End Time: $(date)"
log "Total Runtime: $SECONDS seconds"
log "=========================================="

# Summary for stdout
echo ""
echo "🎉 Telemetry Cleanup Complete!"
echo "📊 Scanned: $TOTAL_FILES files"
echo "🗑️  Deleted: $DELETED_FILES small files"
echo "💾 Freed: $(human_readable $FREED_SPACE)"
echo "📄 Full report: $REPORT_FILE"
