#!/bin/bash
set -e

# =============================================================================
# External Tables Connectivity Test Script
# =============================================================================
# This script validates connectivity to PXF external tables and HDFS data
# =============================================================================

SCRIPT_DIR=$(dirname "$0")

# Load configuration
if [ -f "$SCRIPT_DIR/config.env" ]; then
    source "$SCRIPT_DIR/config.env"
else
    echo "❌ Error: config.env not found!"
    exit 1
fi

# Function to validate configuration
validate_config() {
    echo "Validating configuration..."
    local all_vars_ok=true
    local required_vars=("PGHOST" "PGPORT" "PGUSER" "PGPASSWORD" "HDFS_NAMENODE_HOST" "HDFS_NAMENODE_PORT")

    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            echo "❌ Error: Required variable $var is not set in config.env"
            all_vars_ok=false
        fi
    done

    if [ "$all_vars_ok" = false ]; then
        echo "Please complete config.env before proceeding."
        exit 1
    fi
    echo "Configuration validated successfully."
}

# Function to test database connection
test_db_connection() {
    echo "Testing database connection to $PGHOST:$PGPORT/$PGDATABASE as $PGUSER..."
    
    if psql -c "SELECT 'Connection successful' as status;" > /dev/null 2>&1; then
        echo "✅ Successfully connected to database"
        return 0
    else
        echo "❌ Failed to connect to database"
        echo "   Host: $PGHOST:$PGPORT"
        echo "   Database: $PGDATABASE"
        echo "   User: $PGUSER"
        return 1
    fi
}

# Function to test HDFS connection
test_hdfs_connection() {
    echo "Testing HDFS connection to $HDFS_NAMENODE_HOST:$HDFS_NAMENODE_PORT..."
    
    # Check if HDFS client is available
    if ! command -v hdfs >/dev/null 2>&1; then
        echo "⚠️  HDFS client not available - skipping HDFS connectivity test"
        echo "   External tables will be tested through Greenplum/PXF"
        return 0
    fi
    
    # Construct HDFS URL
    HDFS_URL="${HDFS_PROTOCOL:-hdfs}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
    
    echo "   Testing connection to: $HDFS_URL"
    
    # Test with timeout if gtimeout is available
    if command -v gtimeout >/dev/null 2>&1; then
        if HADOOP_USER_NAME=hdfs gtimeout 10 hdfs dfs -ls "$HDFS_URL/" > /dev/null 2>&1; then
            echo "✅ HDFS connection successful"
            return 0
        else
            echo "⚠️  HDFS connection failed or timed out"
            echo "   This may be normal if HDFS is not accessible from this machine"
            echo "   External tables will be tested through Greenplum/PXF instead"
            return 0
        fi
    else
        echo "⚠️  Timeout command not available - skipping HDFS connectivity test"
        echo "   External tables will be tested through Greenplum/PXF instead"
        return 0
    fi
}

echo "=== External Tables Connectivity Test ==="
validate_config

echo ""
echo "=== Testing Database Connection ==="
if ! test_db_connection; then
    echo "❌ Database connection failed. Cannot proceed with external table tests."
    exit 1
fi

echo ""
echo "=== Testing HDFS Connection ==="
test_hdfs_connection

echo ""
echo "=== Testing PXF External Tables ==="

# Test 1: Check if external tables exist
echo ""
echo "1. Checking if external tables exist..."
TELEMETRY_EXISTS=$(psql -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'vehicle_telemetry_data' AND table_type = 'FOREIGN TABLE';" 2>/dev/null || echo "0")
CRASH_EXISTS=$(psql -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'crash_reports_data' AND table_type = 'FOREIGN TABLE';" 2>/dev/null || echo "0")

if [ "${TELEMETRY_EXISTS// /}" = "1" ]; then
    echo "✅ vehicle_telemetry_data external table exists"
else
    echo "❌ vehicle_telemetry_data external table not found"
fi

if [ "${CRASH_EXISTS// /}" = "1" ]; then
    echo "✅ crash_reports_data external table exists"
else
    echo "❌ crash_reports_data external table not found"
fi

# Test 2: Check external table metadata
echo ""
echo "2. Checking external table metadata..."
psql -c "SELECT schemaname, tablename, location FROM pg_external_table WHERE tablename IN ('vehicle_telemetry_data', 'crash_reports_data');" 2>/dev/null || echo "⚠️  Could not retrieve external table metadata"

# Test 3: Test basic connectivity to external tables
echo ""
echo "3. Testing basic external table connectivity..."

echo "Testing vehicle_telemetry_data..."
TELEMETRY_TEST=$(psql -t -c "SELECT 'SUCCESS' FROM vehicle_telemetry_data LIMIT 1;" 2>/dev/null || echo "FAILED")
if [ "${TELEMETRY_TEST// /}" = "SUCCESS" ]; then
    echo "✅ vehicle_telemetry_data is accessible"
    
    # Get record count estimate
    RECORD_COUNT=$(psql -t -c "SELECT COUNT(*) FROM vehicle_telemetry_data LIMIT 1000;" 2>/dev/null || echo "unknown")
    echo "   Estimated records: ${RECORD_COUNT// /}"
else
    echo "❌ vehicle_telemetry_data is not accessible"
    echo "   This may be normal if no telemetry data exists in HDFS yet"
fi

echo ""
echo "Testing crash_reports_data..."
CRASH_TEST=$(psql -t -c "SELECT 'SUCCESS' FROM crash_reports_data LIMIT 1;" 2>/dev/null || echo "FAILED")
if [ "${CRASH_TEST// /}" = "SUCCESS" ]; then
    echo "✅ crash_reports_data is accessible"
    
    # Get record count estimate
    CRASH_COUNT=$(psql -t -c "SELECT COUNT(*) FROM crash_reports_data LIMIT 1000;" 2>/dev/null || echo "unknown")
    echo "   Estimated records: ${CRASH_COUNT// /}"
else
    echo "❌ crash_reports_data is not accessible"
    echo "   This may be normal if no crash data exists in HDFS yet"
fi

# Test 4: Test analytical views
echo ""
echo "4. Testing analytical views..."
VIEWS=("v_vehicle_telemetry_enriched" "v_high_gforce_events" "v_vehicle_behavior_summary" "v_crash_reports_enriched" "v_emergency_response_queue" "v_crash_patterns" "v_crash_hotspots")

for view in "${VIEWS[@]}"; do
    VIEW_TEST=$(psql -t -c "SELECT 'SUCCESS' FROM $view LIMIT 1;" 2>/dev/null || echo "FAILED")
    if [ "${VIEW_TEST// /}" = "SUCCESS" ]; then
        echo "✅ $view is working"
    else
        echo "❌ $view failed (may be due to missing data)"
    fi
done

# Test 5: PXF Service Status (if available)
echo ""
echo "5. Checking PXF service status..."
if command -v pxf >/dev/null 2>&1; then
    pxf status 2>/dev/null || echo "⚠️  PXF command available but status check failed"
else
    echo "⚠️  PXF command line tool not available on this machine"
fi

# Test 6: Sample data validation
echo ""
echo "6. Sample data validation..."
if [ "${TELEMETRY_TEST// /}" = "SUCCESS" ]; then
    echo "Sample telemetry data:"
    psql -c "SELECT policy_id, vin, timestamp, speed_mph, g_force FROM vehicle_telemetry_data ORDER BY timestamp DESC LIMIT 3;" 2>/dev/null || echo "Could not retrieve sample data"
fi

if [ "${CRASH_TEST// /}" = "SUCCESS" ]; then
    echo "Sample crash data:"
    psql -c "SELECT report_id, policy_id, crash_type, severity_level, risk_score FROM crash_reports_data ORDER BY crash_timestamp DESC LIMIT 3;" 2>/dev/null || echo "Could not retrieve sample crash data"
fi

echo ""
echo "=== Test Summary ==="
echo "Database Connection: ✅"
echo "External Tables Exist: $([ "${TELEMETRY_EXISTS// /}" = "1" ] && [ "${CRASH_EXISTS// /}" = "1" ] && echo "✅" || echo "❌")"
echo "Telemetry Data Access: $([ "${TELEMETRY_TEST// /}" = "SUCCESS" ] && echo "✅" || echo "❌")"
echo "Crash Data Access: $([ "${CRASH_TEST// /}" = "SUCCESS" ] && echo "✅" || echo "❌")"

echo ""
echo "=== Next Steps ==="
if [ "${TELEMETRY_TEST// /}" != "SUCCESS" ]; then
    echo "📋 To populate telemetry data:"
    echo "   1. Ensure the imc-crash-detection app is running and writing to HDFS"
    echo "   2. Verify HDFS path: $HDFS_TELEMETRY_FULL_PATH"
    echo "   3. Check PXF server configuration: $PXF_SERVER_NAME"
fi

echo ""
echo "📋 To run analytics queries:"
echo "   ./connect_remote.sh -f sample_telemetry_queries.sql"
echo ""
echo "📋 To connect interactively:"
echo "   ./connect_remote.sh"