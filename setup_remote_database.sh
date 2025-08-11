#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# =============================================================================
# Remote Database Setup Script
# =============================================================================
# This script sets up the Insurance MegaCorp database schema on a remote
# Greenplum cluster using configuration from config.env
#
# Usage: ./setup_remote_database.sh [environment]
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

# Set default log directory if not configured
if [ -z "$LOG_DIR" ]; then
    echo "⚠️  LOG_DIR not set in config.env. Defaulting to 'logs'."
    export LOG_DIR="$SCRIPT_DIR/logs"
fi
if [ -z "$DB_LOG_FILE" ]; then
    export DB_LOG_FILE="$LOG_DIR/db_setup.log"
fi
if [ -z "$PSQL_VERBOSE" ]; then
    export PSQL_VERBOSE="false"
fi

# Function to validate configuration
validate_config() {
    echo "Validating configuration..."
    local all_vars_ok=true
    local required_vars=("PGHOST" "PGPORT" "PGUSER" "PGPASSWORD" "TARGET_DATABASE" "HDFS_NAMENODE_HOST" "HDFS_NAMENODE_PORT")

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

# Function to create target database
create_target_database() {
    echo "Creating target database '$TARGET_DATABASE' if it doesn't exist..."
    
    # Check if database exists
    DB_EXISTS=$(psql -t -c "SELECT 1 FROM pg_database WHERE datname='$TARGET_DATABASE';" 2>/dev/null | xargs)
    
    if [ "$DB_EXISTS" = "1" ]; then
        echo "✅ Database '$TARGET_DATABASE' already exists"
        return 0
    else
        echo "Creating database '$TARGET_DATABASE'..."
        if psql -c "CREATE DATABASE $TARGET_DATABASE;" > /dev/null 2>&1; then
            echo "✅ Database '$TARGET_DATABASE' created successfully"
            return 0
        else
            echo "❌ Failed to create database '$TARGET_DATABASE'"
            echo "   Please check if you have CREATE DATABASE privileges"
            return 1
        fi
    fi
}

# Function to switch to target database
switch_to_target_database() {
    echo "Switching to target database '$TARGET_DATABASE'..."
    export PGDATABASE="$TARGET_DATABASE"
    echo "✅ Now using database: $TARGET_DATABASE"
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
    HDFS_URL="${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
    
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
        echo "⚠️  Cannot test HDFS safely (gtimeout not available)"
        echo "   Install coreutils: brew install coreutils"
        echo "   Skipping HDFS test - will rely on PXF connectivity"
        return 0
    fi
}

# Override environment if provided as argument
if [ -n "$1" ]; then
    export ENVIRONMENT="$1"
    echo "Using environment: $ENVIRONMENT"
    
    # Reconfigure paths for the specified environment
    case "$ENVIRONMENT" in
        "dev")
            export TARGET_DATABASE="insurance_megacorp_dev"
            export HDFS_TELEMETRY_BASE_PATH="/dev/insurance-megacorp/telemetry-data"
            ;;
        "staging")
            export TARGET_DATABASE="insurance_megacorp_staging"
            export HDFS_TELEMETRY_BASE_PATH="/staging/insurance-megacorp/telemetry-data"
            ;;
        "prod")
            export TARGET_DATABASE="insurance_megacorp_prod"
            export HDFS_TELEMETRY_BASE_PATH="/prod/insurance-megacorp/telemetry-data"
            ;;
    esac
    
    # Rebuild dependent paths
    export HDFS_CRASH_REPORTS_PATH="${HDFS_TELEMETRY_BASE_PATH}/crash-reports"
    export HDFS_TELEMETRY_FULL_PATH="${HDFS_URL}${HDFS_TELEMETRY_BASE_PATH}"
    export HDFS_CRASH_REPORTS_FULL_PATH="${HDFS_URL}${HDFS_CRASH_REPORTS_PATH}"
fi

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Construct HDFS paths if not already set by environment override
if [ -z "$HDFS_URL" ]; then
    export HDFS_URL="${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
fi

if [ -z "$HDFS_TELEMETRY_BASE_PATH" ]; then
    # Set default paths based on environment
    case "${ENVIRONMENT:-dev}" in
        "dev")
            export HDFS_TELEMETRY_BASE_PATH="/dev/insurance-megacorp/telemetry-data"
            ;;
        "staging")
            export HDFS_TELEMETRY_BASE_PATH="/staging/insurance-megacorp/telemetry-data"
            ;;
        "prod")
            export HDFS_TELEMETRY_BASE_PATH="/prod/insurance-megacorp/telemetry-data"
            ;;
        *)
            export HDFS_TELEMETRY_BASE_PATH="/dev/insurance-megacorp/telemetry-data"
            ;;
    esac
fi

# Construct dependent paths
if [ -z "$HDFS_CRASH_REPORTS_PATH" ]; then
    export HDFS_CRASH_REPORTS_PATH="${HDFS_TELEMETRY_BASE_PATH}/crash-reports"
fi

if [ -z "$HDFS_TELEMETRY_FULL_PATH" ]; then
    export HDFS_TELEMETRY_FULL_PATH="${HDFS_URL}${HDFS_TELEMETRY_BASE_PATH}"
fi

if [ -z "$HDFS_CRASH_REPORTS_FULL_PATH" ]; then
    export HDFS_CRASH_REPORTS_FULL_PATH="${HDFS_URL}${HDFS_CRASH_REPORTS_PATH}"
fi

echo "=== Insurance MegaCorp Remote Database Setup ==="
validate_config

# Test database connectivity to default database
echo ""
echo "Testing database connectivity..."
if ! test_db_connection; then
    echo "❌ Cannot connect to default database. Please check your configuration."
    exit 1
fi

# Create target database
echo ""
echo "Creating target database if needed..."
if ! create_target_database; then
    echo "❌ Failed to create target database. Exiting."
    exit 1
fi

# Switch to target database for schema creation
switch_to_target_database

# Test HDFS connectivity (optional)
echo ""
echo "Testing HDFS connectivity..."
test_hdfs_connection

echo ""
echo "=== Starting Database Schema Creation ==="

# Generate external tables with current configuration
echo "Generating external table definitions with current HDFS paths..."
"$SCRIPT_DIR/generate_external_tables.sh"

# Create the database schema
echo "Creating database schema..."
if [ "$PSQL_VERBOSE" = "true" ]; then
    psql -f "$SCRIPT_DIR/create_schema.sql" | tee "$DB_LOG_FILE"
else
    psql -f "$SCRIPT_DIR/create_schema.sql"
fi

echo ""
echo "=== Loading Sample Data ==="
if [ "$PSQL_VERBOSE" = "true" ]; then
    psql -f "$SCRIPT_DIR/load_sample_data.sql" | tee -a "$DB_LOG_FILE"
else
    psql -f "$SCRIPT_DIR/load_sample_data.sql"
fi

echo ""
echo "=== Validating External Tables ==="
echo "Testing external table connectivity..."
psql -c "SELECT 'vehicle_telemetry_data' as table_name, COUNT(*) as estimated_records FROM vehicle_telemetry_data LIMIT 1;" 2>/dev/null || echo "⚠️  External table validation requires telemetry data in HDFS"

echo ""
echo "✅ Database Setup Complete!"
echo ""
echo "=== Connection Information ==="
echo "Database: $PGUSER@$PGHOST:$PGPORT/$TARGET_DATABASE"
echo "Environment: $ENVIRONMENT"
echo "HDFS Telemetry Path: $HDFS_TELEMETRY_FULL_PATH"
echo ""
echo "=== Next Steps ==="
echo "1. To connect interactively:"
echo "   source config.env && psql"
echo ""
echo "2. To run sample telemetry queries:"
echo "   source config.env && psql -f sample_telemetry_queries.sql"
echo ""
echo "3. To drop all tables:"
echo "   source config.env && psql -f drop_tables.sql"
echo ""
echo "4. To validate PXF external tables:"
echo "   source config.env && ./test_external_tables.sh"
echo ""

# Optional: Run basic validation queries
read -p "Would you like to run basic validation queries now? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Running validation queries..."
    psql -f "$SCRIPT_DIR/sample_telemetry_queries.sql" | head -50
fi