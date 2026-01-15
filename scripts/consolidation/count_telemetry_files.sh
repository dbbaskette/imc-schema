#!/usr/bin/env bash
# =============================================================================
# Telemetry File Count Report
# =============================================================================
# Scans each date partition and reports file counts incrementally
# Saves results and shows diff from previous run
#
# Usage:
#   ./count_telemetry_files.sh                  # All dates
#   ./count_telemetry_files.sh --month 2025-08  # Only August 2025
#   ./count_telemetry_files.sh --year 2025      # All of 2025
# =============================================================================

set -euo pipefail

# Parse arguments
DATE_FILTER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --month|-m)
            DATE_FILTER="$2"
            if [[ ! "$DATE_FILTER" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
                echo "Error: --month requires format YYYY-MM (e.g., 2025-08)"
                exit 1
            fi
            shift 2
            ;;
        --year|-y)
            DATE_FILTER="$2"
            if [[ ! "$DATE_FILTER" =~ ^[0-9]{4}$ ]]; then
                echo "Error: --year requires format YYYY (e.g., 2025)"
                exit 1
            fi
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--month YYYY-MM] [--year YYYY]"
            echo ""
            echo "Options:"
            echo "  --month, -m    Filter by month (e.g., 2025-08)"
            echo "  --year, -y     Filter by year (e.g., 2025)"
            echo ""
            echo "Examples:"
            echo "  $0                    # All dates"
            echo "  $0 --month 2025-08    # Only August 2025"
            echo "  $0 -m 2025-12         # Only December 2025"
            echo "  $0 --year 2025        # All of 2025"
            echo "  $0 -y 2026            # All of 2026"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage"
            exit 1
            ;;
    esac
done

# Load configuration (find repo root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/config.env"

HDFS_PATH="/insurance-megacorp/telemetry-data-v2"
REPORT_DIR="${LOG_DIR:-./logs}"
CURRENT_FILE="$REPORT_DIR/telemetry_counts_current.txt"
PREVIOUS_FILE="$REPORT_DIR/telemetry_counts_previous.txt"
TOTAL_FILES=0

# Create report directory
mkdir -p "$REPORT_DIR"

# Only save/compare when running without filter
if [[ -z "$DATE_FILTER" ]]; then
    # Save previous run if exists
    if [[ -f "$CURRENT_FILE" ]]; then
        cp "$CURRENT_FILE" "$PREVIOUS_FILE"
    fi
fi

# Create temp file for current run
TEMP_FILE=$(mktemp)

echo "========================================"
echo "Telemetry File Count Report"
echo "========================================"
echo "HDFS Path: $HDFS_PATH"
if [[ -n "$DATE_FILTER" ]]; then
    echo "Filter: $DATE_FILTER*"
fi
echo "Started: $(date)"
echo "========================================"
echo ""

# Header - show diff column only when no filter and previous exists
SHOW_DIFF=false
if [[ -z "$DATE_FILTER" ]] && [[ -f "$PREVIOUS_FILE" ]]; then
    SHOW_DIFF=true
    printf "%-20s %10s %10s %15s\n" "DATE" "FILES" "DIFF" "RUNNING TOTAL"
    printf "%-20s %10s %10s %15s\n" "----" "-----" "----" "-------------"
else
    printf "%-20s %10s %15s\n" "DATE" "FILES" "RUNNING TOTAL"
    printf "%-20s %10s %15s\n" "----" "-----" "-------------"
fi

# Get list of date directories (sorted by date, with optional filter)
hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep "date=" | awk '{print $NF}' | sed 's/.*date=//' | sort | while read -r date_part; do
    # Apply month/year filter if specified
    if [[ -n "$DATE_FILTER" ]] && [[ ! "$date_part" == "$DATE_FILTER"* ]]; then
        continue
    fi

    dir_path="$HDFS_PATH/date=$date_part"

    # Count parquet files in this directory
    file_count=$(hdfs dfs -ls "$dir_path" 2>/dev/null | grep -c "\.parquet$" || echo "0")

    # Save to temp file (date,count format)
    echo "$date_part,$file_count" >> "$TEMP_FILE"

    # Calculate running total (read from temp file)
    TOTAL_FILES=$(awk -F',' '{sum+=$2} END {print sum}' "$TEMP_FILE")

    # Look up previous count from file (grep-based, compatible with bash 3)
    prev_count=0
    if [[ -f "$PREVIOUS_FILE" ]]; then
        prev_line=$(grep "^${date_part}," "$PREVIOUS_FILE" 2>/dev/null || echo "")
        if [[ -n "$prev_line" ]]; then
            prev_count=$(echo "$prev_line" | cut -d',' -f2)
        fi
    fi
    diff=$((file_count - prev_count))

    # Format diff with sign
    if [[ $diff -gt 0 ]]; then
        diff_str="+$diff"
    elif [[ $diff -lt 0 ]]; then
        diff_str="$diff"
    else
        diff_str="-"
    fi

    # Print incrementally
    if [[ "$SHOW_DIFF" == "true" ]]; then
        printf "%-20s %10d %10s %15d\n" "$date_part" "$file_count" "$diff_str" "$TOTAL_FILES"
    else
        printf "%-20s %10d %15d\n" "$date_part" "$file_count" "$TOTAL_FILES"
    fi
done

# Only save results when running without filter
if [[ -z "$DATE_FILTER" ]]; then
    mv "$TEMP_FILE" "$CURRENT_FILE"
else
    rm -f "$TEMP_FILE"
fi

# Final summary
echo ""
echo "========================================"

# Show summary diff only when no filter
if [[ -z "$DATE_FILTER" ]] && [[ -f "$PREVIOUS_FILE" ]]; then
    PREV_TOTAL=$(awk -F',' '{sum+=$2} END {print sum}' "$PREVIOUS_FILE")
    CURR_TOTAL=$(awk -F',' '{sum+=$2} END {print sum}' "$CURRENT_FILE")
    TOTAL_DIFF=$((CURR_TOTAL - PREV_TOTAL))

    echo "Previous Total: $PREV_TOTAL files"
    echo "Current Total:  $CURR_TOTAL files"
    if [[ $TOTAL_DIFF -gt 0 ]]; then
        echo "Change:         +$TOTAL_DIFF files"
    elif [[ $TOTAL_DIFF -lt 0 ]]; then
        echo "Change:         $TOTAL_DIFF files"
    else
        echo "Change:         No change"
    fi
    echo ""

    # Show new dates
    NEW_DATES=$(comm -13 <(cut -d',' -f1 "$PREVIOUS_FILE" | sort) <(cut -d',' -f1 "$CURRENT_FILE" | sort))
    if [[ -n "$NEW_DATES" ]]; then
        echo "New date partitions:"
        echo "$NEW_DATES" | while read -r d; do
            echo "  + $d"
        done
        echo ""
    fi

    # Show removed dates
    REMOVED_DATES=$(comm -23 <(cut -d',' -f1 "$PREVIOUS_FILE" | sort) <(cut -d',' -f1 "$CURRENT_FILE" | sort))
    if [[ -n "$REMOVED_DATES" ]]; then
        echo "Removed date partitions:"
        echo "$REMOVED_DATES" | while read -r d; do
            echo "  - $d"
        done
        echo ""
    fi
fi

echo "Completed: $(date)"
if [[ -z "$DATE_FILTER" ]]; then
    echo "Results saved to: $CURRENT_FILE"
fi
echo "========================================"
