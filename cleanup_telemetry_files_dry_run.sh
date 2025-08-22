#!/bin/bash
# =============================================================================
# Telemetry Data Cleanup Script - DRY RUN MODE
# =============================================================================
# Scans telemetry_data_v2 directory and reports files ≤10KB WITHOUT deleting
# Safe to run for analysis before actual cleanup
# =============================================================================

set -euo pipefail

# Load configuration
source config.env

# Configuration
SIZE_THRESHOLD_KB=10
SIZE_THRESHOLD_BYTES=$((SIZE_THRESHOLD_KB * 1024))
TELEMETRY_BASE_PATH="${1:-hdfs://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}/insurance-megacorp/telemetry-data-v2}"
REPORT_DIR="${LOG_DIR:-./logs}"
REPORT_FILE="$REPORT_DIR/telemetry_cleanup_dryrun_$(date +%Y%m%d_%H%M%S).log"

# Create report directory
mkdir -p "$REPORT_DIR"

# Initialize counters
TOTAL_FILES=0
SMALL_FILES=0
TOTAL_SIZE_BEFORE=0
POTENTIAL_FREED_SPACE=0

# Arrays to store file information
declare -a SMALL_FILE_LIST=()

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

# =============================================================================
# Main Execution
# =============================================================================

log "=========================================="
log "TELEMETRY DATA CLEANUP ANALYSIS (DRY RUN)"
log "=========================================="
log "Start Time: $(date)"
log "Target Path: $TELEMETRY_BASE_PATH"
log "Size Threshold: ${SIZE_THRESHOLD_KB}KB (${SIZE_THRESHOLD_BYTES} bytes)"
log "Report File: $REPORT_FILE"
log ""
log "🔒 DRY RUN MODE - NO FILES WILL BE DELETED"
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
        if [[ -n "$line" ]]; then
            # Extract file size (6th field) and filename (last field)
            FILE_SIZE=$(echo "$line" | awk '{print $5}')
            FILE_PATH=$(echo "$line" | awk '{print $NF}')
            
            TOTAL_FILES=$((TOTAL_FILES + 1))
            TOTAL_SIZE_BEFORE=$((TOTAL_SIZE_BEFORE + FILE_SIZE))
            
            if [ "$FILE_SIZE" -le "$SIZE_THRESHOLD_BYTES" ]; then
                SMALL_FILES=$((SMALL_FILES + 1))
                POTENTIAL_FREED_SPACE=$((POTENTIAL_FREED_SPACE + FILE_SIZE))
                SMALL_FILE_LIST+=("$FILE_PATH ($(human_readable $FILE_SIZE))")
                log "📋 Would delete: $FILE_PATH ($(human_readable $FILE_SIZE))"
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
                POTENTIAL_FREED_SPACE=$((POTENTIAL_FREED_SPACE + FILE_SIZE))
                SMALL_FILE_LIST+=("$file ($(human_readable $FILE_SIZE))")
                log "📋 Would delete: $file ($(human_readable $FILE_SIZE))"
            fi
        fi
    done < <(find "$TELEMETRY_BASE_PATH" -type f -print0)
fi

# =============================================================================
# Generate Analysis Report
# =============================================================================

log ""
log "=========================================="
log "CLEANUP ANALYSIS SUMMARY"
log "=========================================="
log "📁 Total Files Found: $TOTAL_FILES"
log "📏 Files ≤${SIZE_THRESHOLD_KB}KB: $SMALL_FILES"
log "🗑️  Files That Would Be Deleted: $SMALL_FILES"
log ""
log "💾 Storage Impact Analysis:"
log "   • Current Total Size: $(human_readable $TOTAL_SIZE_BEFORE)"
log "   • Potential Space Freed: $(human_readable $POTENTIAL_FREED_SPACE)"
if [ $TOTAL_SIZE_BEFORE -gt 0 ]; then
    log "   • Potential Reduction: $(echo "scale=2; $POTENTIAL_FREED_SPACE * 100 / $TOTAL_SIZE_BEFORE" | bc)%"
else
    log "   • Potential Reduction: 0%"
fi
log ""

# Detailed file listings
if [ ${#SMALL_FILE_LIST[@]} -gt 0 ]; then
    log "=========================================="
    log "SMALL FILES THAT WOULD BE DELETED (${#SMALL_FILE_LIST[@]} total)"
    log "=========================================="
    for file in "${SMALL_FILE_LIST[@]}"; do
        log "🗑️  $file"
    done
    log ""
fi

# Directory structure analysis
log "=========================================="
log "DIRECTORY STRUCTURE ANALYSIS"
log "=========================================="

if [[ "$TELEMETRY_BASE_PATH" == hdfs://* ]]; then
    # HDFS directory analysis
    log "📂 Directory sizes (HDFS):"
    HADOOP_USER_NAME=hdfs hdfs dfs -du -s "$TELEMETRY_BASE_PATH"/* 2>/dev/null | while read size path; do
        log "   $(human_readable $size) - $(basename "$path")"
    done || log "   Unable to analyze directory structure"
else
    # Local directory analysis
    log "📂 Directory sizes (Local):"
    du -h -d 1 "$TELEMETRY_BASE_PATH" 2>/dev/null | sort -hr | while read size path; do
        log "   $size - $(basename "$path")"
    done || log "   Unable to analyze directory structure"
fi

# File size distribution
log ""
log "📊 File size distribution:"
small_kb_count=0
tiny_files_count=0

for ((i=0; i<${#SMALL_FILE_LIST[@]}; i++)); do
    file_info="${SMALL_FILE_LIST[$i]}"
    # Extract size from file info - look for patterns like (1.23KB) or (456B)
    if [[ $file_info =~ \(([0-9.]+)(KB|B)\) ]]; then
        size_value="${BASH_REMATCH[1]}"
        size_unit="${BASH_REMATCH[2]}"
        if [[ $size_unit == "KB" ]]; then
            small_kb_count=$((small_kb_count + 1))
        else
            tiny_files_count=$((tiny_files_count + 1))
        fi
    fi
done

if [ $tiny_files_count -gt 0 ]; then
    log "   < 1KB: $tiny_files_count files"
fi
if [ $small_kb_count -gt 0 ]; then
    log "   1KB - 10KB: $small_kb_count files"
fi

log ""
log "=========================================="
log "RECOMMENDATIONS"
log "=========================================="

if [ $SMALL_FILES -eq 0 ]; then
    log "✅ No small files found - telemetry data appears healthy"
elif [ $SMALL_FILES -lt 10 ]; then
    log "✅ Few small files ($SMALL_FILES) - acceptable data quality"
    log "💡 Monitor data pipeline to prevent accumulation"
elif [ $SMALL_FILES -lt 100 ]; then
    log "⚠️  Moderate number of small files ($SMALL_FILES)"
    log "💡 Consider running cleanup to free $(human_readable $POTENTIAL_FREED_SPACE)"
    log "💡 Investigate data ingestion process for optimization"
else
    log "🚨 High number of small files ($SMALL_FILES)"
    log "💡 Cleanup recommended to free $(human_readable $POTENTIAL_FREED_SPACE)"
    log "💡 Urgent: Review and fix data pipeline to prevent small file creation"
fi

log ""
log "🔄 To execute the actual cleanup, run:"
log "   ./cleanup_telemetry_files.sh"
log ""
log "End Time: $(date)"
log "Total Runtime: $SECONDS seconds"
log "=========================================="

# Summary for stdout
echo ""
echo "🔍 Telemetry Analysis Complete!"
echo "📊 Found: $TOTAL_FILES files total"
echo "🗑️  Small files (≤${SIZE_THRESHOLD_KB}KB): $SMALL_FILES"
echo "💾 Potential space freed: $(human_readable $POTENTIAL_FREED_SPACE)"
echo "📄 Full report: $REPORT_FILE"
echo ""
if [ $SMALL_FILES -gt 0 ]; then
    echo "To execute cleanup: ./cleanup_telemetry_files.sh"
else
    echo "✅ No cleanup needed!"
fi