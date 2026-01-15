#!/bin/bash
set -e

# =============================================================================
# External Tables Connectivity Test Script
# =============================================================================
# This script validates connectivity to PXF external tables and HDFS data
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load configuration
if [ -f "$REPO_ROOT/config.env" ]; then
    source "$REPO_ROOT/config.env"
else
    echo "❌ Error: config.env not found at $REPO_ROOT/config.env"
    exit 1
fi

# Use the target database (insurance_megacorp) for external tables
export PGDATABASE="${TARGET_DATABASE:-insurance_megacorp}"

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
TELEMETRY_V2_EXISTS=$(psql -t -c "SELECT COUNT(*) FROM information_schema.foreign_tables WHERE foreign_table_name = 'vehicle_telemetry_data_v2';" 2>/dev/null || echo "0")
TELEMETRY_EXISTS=$(psql -t -c "SELECT COUNT(*) FROM information_schema.foreign_tables WHERE foreign_table_name = 'vehicle_telemetry_data';" 2>/dev/null || echo "0")
CRASH_EXISTS=$(psql -t -c "SELECT COUNT(*) FROM information_schema.foreign_tables WHERE foreign_table_name = 'crash_reports_data';" 2>/dev/null || echo "0")

if [ "${TELEMETRY_V2_EXISTS// /}" -ge "1" ]; then
    echo "✅ vehicle_telemetry_data_v2 external table exists"
else
    echo "❌ vehicle_telemetry_data_v2 external table not found"
fi

if [ "${TELEMETRY_EXISTS// /}" -ge "1" ]; then
    echo "✅ vehicle_telemetry_data external table exists"
else
    echo "⚠️  vehicle_telemetry_data external table not found (optional)"
fi

if [ "${CRASH_EXISTS// /}" -ge "1" ]; then
    echo "✅ crash_reports_data external table exists"
else
    echo "⚠️  crash_reports_data external table not found (optional)"
fi

# Test 2: Check external table metadata
echo ""
echo "2. Checking external table metadata..."
psql -c "SELECT foreign_table_schema, foreign_table_name, foreign_server_name FROM information_schema.foreign_tables WHERE foreign_table_name IN ('vehicle_telemetry_data_v2', 'vehicle_telemetry_data', 'crash_reports_data');" 2>/dev/null || echo "⚠️  Could not retrieve external table metadata"

# Test 3: Test basic connectivity to external tables
echo ""
echo "3. Testing basic external table connectivity..."

# Test vehicle_telemetry_data_v2 (primary)
echo "Testing vehicle_telemetry_data_v2 (primary telemetry table)..."
TELEMETRY_V2_TEST=$(psql -t -c "SELECT 'SUCCESS' FROM vehicle_telemetry_data_v2 LIMIT 1;" 2>/dev/null || echo "FAILED")
if [ "${TELEMETRY_V2_TEST// /}" = "SUCCESS" ]; then
    echo "✅ vehicle_telemetry_data_v2 is accessible"
else
    echo "❌ vehicle_telemetry_data_v2 is not accessible"
    echo "   This may be normal if no telemetry data exists in HDFS yet"
fi

# Test vehicle_telemetry_data (legacy)
echo ""
echo "Testing vehicle_telemetry_data (legacy)..."
TELEMETRY_TEST=$(psql -t -c "SELECT 'SUCCESS' FROM vehicle_telemetry_data LIMIT 1;" 2>/dev/null || echo "FAILED")
if [ "${TELEMETRY_TEST// /}" = "SUCCESS" ]; then
    echo "✅ vehicle_telemetry_data is accessible"
else
    echo "⚠️  vehicle_telemetry_data is not accessible (legacy table)"
fi

echo ""
echo "Testing crash_reports_data..."
CRASH_TEST=$(psql -t -c "SELECT 'SUCCESS' FROM crash_reports_data LIMIT 1;" 2>/dev/null || echo "FAILED")
if [ "${CRASH_TEST// /}" = "SUCCESS" ]; then
    echo "✅ crash_reports_data is accessible"
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

# Test 5: Performance Testing with Timing
echo ""
echo "5. Performance testing (COUNT queries with timing)..."

if [ "${TELEMETRY_V2_TEST// /}" = "SUCCESS" ]; then
    echo ""
    echo "Testing vehicle_telemetry_data_v2 COUNT(*) performance..."
    echo "   Running: SELECT COUNT(*) FROM vehicle_telemetry_data_v2"

    START_TIME=$(date +%s.%N)
    V2_COUNT=$(psql -t -c "SELECT COUNT(*) FROM vehicle_telemetry_data_v2;" 2>/dev/null || echo "ERROR")
    END_TIME=$(date +%s.%N)

    if [ "${V2_COUNT// /}" != "ERROR" ]; then
        ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
        echo "   ✅ Count: ${V2_COUNT// /} records"
        echo "   ⏱️  Time: ${ELAPSED} seconds"
    else
        echo "   ❌ COUNT query failed"
    fi
fi

if [ "${CRASH_TEST// /}" = "SUCCESS" ]; then
    echo ""
    echo "Testing crash_reports_data COUNT(*) performance..."
    echo "   Running: SELECT COUNT(*) FROM crash_reports_data"

    START_TIME=$(date +%s.%N)
    CRASH_COUNT=$(psql -t -c "SELECT COUNT(*) FROM crash_reports_data;" 2>/dev/null || echo "ERROR")
    END_TIME=$(date +%s.%N)

    if [ "${CRASH_COUNT// /}" != "ERROR" ]; then
        ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
        echo "   ✅ Count: ${CRASH_COUNT// /} records"
        echo "   ⏱️  Time: ${ELAPSED} seconds"
    else
        echo "   ❌ COUNT query failed"
    fi
fi

# Test 6: PXF Service Status (if available)
echo ""
echo "6. Checking PXF service status..."
if command -v pxf >/dev/null 2>&1; then
    pxf status 2>/dev/null || echo "⚠️  PXF command available but status check failed"
else
    echo "⚠️  PXF command line tool not available on this machine"
fi

# Test 7: Sample data validation
echo ""
echo "7. Sample data validation..."
if [ "${TELEMETRY_V2_TEST// /}" = "SUCCESS" ]; then
    echo "Sample telemetry v2 data:"
    psql -c "SELECT policy_id, vin, event_time, speed_mph, g_force FROM vehicle_telemetry_data_v2 ORDER BY event_time DESC LIMIT 3;" 2>/dev/null || echo "Could not retrieve sample data"
fi

if [ "${CRASH_TEST// /}" = "SUCCESS" ]; then
    echo "Sample crash data:"
    psql -c "SELECT report_id, policy_id, crash_type, risk_score FROM crash_reports_data ORDER BY crash_timestamp DESC LIMIT 3;" 2>/dev/null || echo "Could not retrieve sample crash data"
fi

echo ""
echo "=== Test Summary ==="
echo "Database Connection: ✅"
echo "Telemetry V2 Table Exists: $([ "${TELEMETRY_V2_EXISTS// /}" -ge "1" ] && echo "✅" || echo "❌")"
echo "Telemetry V2 Data Access: $([ "${TELEMETRY_V2_TEST// /}" = "SUCCESS" ] && echo "✅" || echo "❌")"
echo "Telemetry V2 Record Count: ${V2_COUNT:-N/A}"
echo "Crash Data Access: $([ "${CRASH_TEST// /}" = "SUCCESS" ] && echo "✅" || echo "❌")"

echo ""
echo "=== Next Steps ==="
if [ "${TELEMETRY_V2_TEST// /}" != "SUCCESS" ]; then
    echo "📋 To populate telemetry data:"
    echo "   1. Ensure the imc-crash-detection app is running and writing to HDFS"
    echo "   2. Verify HDFS path: /insurance-megacorp/telemetry-data-v2/"
    echo "   3. Check PXF server configuration: $PXF_SERVER_NAME"
    echo ""
    echo "📋 If external table has stale metadata after consolidation:"
    echo "   1. Drop and recreate the external table"
    echo "   2. Or run: ./connect_remote.sh -f 06_create_external_tables_v2.sql"
fi

echo ""
echo "📋 To run analytics queries:"
echo "   ./connect_remote.sh -f sample_telemetry_queries.sql"
echo ""
echo "📋 To connect interactively:"
echo "   ./connect_remote.sh"