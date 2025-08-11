#!/bin/bash
set -e

# =============================================================================
# HDFS Data Loader Script
# =============================================================================
# This script loads local data files to HDFS using configuration from config.env
#
# Usage: ./load_hdfs.sh [environment]
# =============================================================================

SCRIPT_DIR=$(dirname "$0")

# Load configuration
if [ -f "$SCRIPT_DIR/config.env" ]; then
    echo "Loading configuration from config.env..."
    source "$SCRIPT_DIR/config.env"
else
    echo "❌ Error: config.env not found!"
    echo "Please copy config.env.example to config.env and update with your settings."
    exit 1
fi

# Override environment if provided as argument
if [ -n "$1" ]; then
    export ENVIRONMENT="$1"
    echo "Loading data for environment: $ENVIRONMENT"
else
    echo "Loading data for environment: $ENVIRONMENT"
fi

echo ""
echo "=== Insurance MegaCorp HDFS Data Loader ==="
echo "Target HDFS: ${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
echo "Environment: $ENVIRONMENT"
echo ""

# Construct HDFS URL
HDFS_URL="${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"

# Check if HDFS client is available
if ! command -v hdfs >/dev/null 2>&1; then
    echo "❌ HDFS client not found!"
    echo "Please install Hadoop client: brew install hadoop"
    exit 1
fi

echo "✅ HDFS client available: $(hdfs version | head -1)"
echo ""

# =============================================================================
# LOAD POLICIES DATA
# =============================================================================
echo "📁 Loading Policies Data"
echo "==============================================" 

# Check if local policies directory exists
if [ ! -d "$SCRIPT_DIR/policies" ]; then
    echo "❌ Local policies directory not found: $SCRIPT_DIR/policies"
    echo "Please ensure the policies directory exists with PDF files."
    exit 1
fi

# Count local policy files
LOCAL_POLICY_COUNT=$(find "$SCRIPT_DIR/policies" -name "*.pdf" | wc -l)
echo "Found $LOCAL_POLICY_COUNT policy files locally"

if [ "$LOCAL_POLICY_COUNT" -eq 0 ]; then
    echo "❌ No PDF files found in policies directory"
    exit 1
fi

echo "Local policy files:"
find "$SCRIPT_DIR/policies" -name "*.pdf" -exec basename {} \; | sed 's/^/   /'

echo ""
echo "Creating HDFS policies directory..."
if HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p "$HDFS_URL/policies" 2>/dev/null; then
    echo "✅ HDFS policies directory created"
else
    echo "ℹ️  HDFS policies directory already exists or creation failed"
fi

echo ""
echo "Uploading policy files to HDFS..."

# Upload each PDF file
SUCCESS_COUNT=0
TOTAL_FILES=0

for pdf_file in "$SCRIPT_DIR/policies"/*.pdf; do
    if [ -f "$pdf_file" ]; then
        TOTAL_FILES=$((TOTAL_FILES + 1))
        filename=$(basename "$pdf_file")
        
        echo "   Uploading: $filename"
        
        if HADOOP_USER_NAME=hdfs hdfs dfs -put "$pdf_file" "$HDFS_URL/policies/" 2>/dev/null; then
            echo "   ✅ Success: $filename"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo "   ❌ Failed: $filename"
        fi
    fi
done

echo ""
echo "Upload Summary:"
echo "   Total files: $TOTAL_FILES"
echo "   Successful: $SUCCESS_COUNT"
echo "   Failed: $((TOTAL_FILES - SUCCESS_COUNT))"

# Verify uploaded files
echo ""
echo "Verifying uploaded files in HDFS..."
if HADOOP_USER_NAME=hdfs hdfs dfs -ls "$HDFS_URL/policies/" 2>/dev/null; then
    HDFS_FILE_COUNT=$(HADOOP_USER_NAME=hdfs hdfs dfs -ls "$HDFS_URL/policies/" 2>/dev/null | grep -c "\.pdf")
    echo ""
    echo "✅ Verification complete: $HDFS_FILE_COUNT PDF files found in HDFS"
else
    echo "❌ Failed to verify HDFS upload"
fi

# =============================================================================
# CREATE DIRECTORY STRUCTURE FOR TELEMETRY DATA
# =============================================================================
echo ""
echo "📊 Preparing Telemetry Data Structure"
echo "==============================================" 

# Create directory structure for telemetry data (to be used by crash detection app)
TELEMETRY_BASE_PATH="/insurance-megacorp/telemetry-data"

echo "Creating telemetry data directory structure..."
if HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p "$HDFS_URL$TELEMETRY_BASE_PATH" 2>/dev/null; then
    echo "✅ Telemetry base directory created: $TELEMETRY_BASE_PATH"
else
    echo "ℹ️  Telemetry base directory already exists: $TELEMETRY_BASE_PATH"
fi

# Create some sample partition directories
CURRENT_YEAR=$(date +%Y)
CURRENT_MONTH=$(date +%m)

for policy_id in 100001 100002 100003; do
    PARTITION_PATH="$TELEMETRY_BASE_PATH/policy_id=$policy_id/year=$CURRENT_YEAR/month=$CURRENT_MONTH"
    
    if HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p "$HDFS_URL$PARTITION_PATH" 2>/dev/null; then
        echo "✅ Created partition: $PARTITION_PATH"
    else
        echo "ℹ️  Partition already exists: $PARTITION_PATH"
    fi
done

echo ""
echo "📋 Final HDFS Structure:"
echo "==============================================" 
echo "Root directory contents:"
HADOOP_USER_NAME=hdfs hdfs dfs -ls "$HDFS_URL/" 2>/dev/null | sed 's/^/   /'

echo ""
echo "Policies directory contents:"
HADOOP_USER_NAME=hdfs hdfs dfs -ls "$HDFS_URL/policies/" 2>/dev/null | sed 's/^/   /'

echo ""
echo "Telemetry directory structure:"
HADOOP_USER_NAME=hdfs hdfs dfs -ls -R "$HDFS_URL/insurance-megacorp/" 2>/dev/null | sed 's/^/   /'

echo ""
echo "🎉 HDFS data loading complete!"
echo ""
echo "Next steps:"
echo "1. Run database setup: ./setup_remote_database.sh $ENVIRONMENT"
echo "2. Test external tables: ./test_external_tables.sh"
echo "3. Start crash detection app to generate telemetry data"