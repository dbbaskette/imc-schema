#!/bin/bash
# Simple consolidation preview using SSH to HDFS host

HDFS_HOST="hdfs@big-data-005.kuhn-labs.com"
HDFS_PATH="/insurance-megacorp/telemetry-data-v2/date=2025-08-15"

echo "🔍 Consolidation Preview for 2025-08-15"
echo "========================================"
echo

echo "📁 Files in $HDFS_PATH:"
files_output=$(ssh $HDFS_HOST "hdfs dfs -ls $HDFS_PATH 2>/dev/null | grep 'telemetry-.*\.parquet'" 2>/dev/null)

if [[ -z "$files_output" ]]; then
    echo "❌ No telemetry Parquet files found"
    exit 1
fi

echo "$files_output" | head -10
echo

# Count files and calculate total size
file_count=$(echo "$files_output" | wc -l | tr -d ' ')
total_bytes=$(echo "$files_output" | awk '{sum += $5} END {print sum}')

echo "📊 Summary:"
echo "  📁 Total Files: $file_count"
echo "  📏 Total Size: $(echo $total_bytes | awk '{printf "%.2f MB", $1/1024/1024}')"
echo "  📐 Average Size: $(echo "$total_bytes $file_count" | awk '{printf "%.1f KB", $1/$2/1024}')"

echo
echo "⚡ Consolidation Impact:"
echo "  🎯 Would consolidate $file_count files into 1 large file"

if [[ $file_count -gt 10 ]]; then
    echo "  🚀 Query performance improvement: ~${file_count}x faster"
    echo "  ✅ Excellent candidate for consolidation!"
elif [[ $file_count -ge 5 ]]; then
    echo "  🚀 Query performance improvement: ~$((file_count/2))x faster"
    echo "  ✅ Good candidate for consolidation!"
else
    echo "  ⚠️  Below recommended threshold (need ≥5 files)"
fi

echo
echo "🛠️ Next Steps:"
echo "  1. Install Python dependencies: pip install pandas pyarrow hdfs3"
echo "  2. Run actual consolidation: ./consolidate_telemetry.sh --date 2025-08-15 --dry-run"
echo "  3. If satisfied, run: ./consolidate_telemetry.sh --date 2025-08-15"
