#!/bin/bash
set -e

# =============================================================================
# Connectivity Test Script
# =============================================================================
# This script tests all connections using configuration from config.env:
# - Greenplum database connectivity
# - HDFS connectivity (if hdfs client available)
# - Network connectivity to hosts
# - Basic validation of configuration parameters
#
# Usage: ./test_connectivity.sh [environment]
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
    echo "Testing environment: $ENVIRONMENT"
    echo ""
fi

echo "=== Insurance MegaCorp Connectivity Test ==="
echo "Test started at: $(date)"
echo ""

# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================
echo "🔧 Configuration Validation"
echo "=============================================="

# Check required variables
REQUIRED_VARS=("PGHOST" "PGPORT" "PGUSER" "PGPASSWORD" "HDFS_NAMENODE_HOST" "HDFS_NAMENODE_PORT")
CONFIG_VALID=true

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Missing required variable: $var"
        CONFIG_VALID=false
    else
        echo "✅ $var is set"
    fi
done

if [ "$CONFIG_VALID" = false ]; then
    echo ""
    echo "❌ Configuration validation failed. Please check your config.env file."
    exit 1
fi

echo ""
echo "📋 Current Configuration:"
echo "   Environment: $ENVIRONMENT"
echo "   GP Host: $PGHOST:$PGPORT"
echo "   GP User: $PGUSER"
echo "   GP Database: $PGDATABASE"
echo "   HDFS NameNode: $HDFS_NAMENODE_HOST:$HDFS_NAMENODE_PORT"
echo "   PXF Server: $PXF_SERVER_NAME"
echo ""

# =============================================================================
# NETWORK CONNECTIVITY TESTS
# =============================================================================
echo "🌐 Network Connectivity Tests"
echo "=============================================="

# Test Greenplum host connectivity
echo "Testing network connectivity to Greenplum host..."
if ping -c 3 -W 5 "$PGHOST" > /dev/null 2>&1; then
    echo "✅ $PGHOST is reachable"
else
    echo "❌ $PGHOST is not reachable"
fi

# Test HDFS NameNode host connectivity
echo "Testing network connectivity to HDFS NameNode..."
if ping -c 3 -W 5 "$HDFS_NAMENODE_HOST" > /dev/null 2>&1; then
    echo "✅ $HDFS_NAMENODE_HOST is reachable"
else
    echo "❌ $HDFS_NAMENODE_HOST is not reachable"
fi

echo "Skipping port scans (often blocked by firewalls - will test via application connections instead)..."

# Set these to unknown initially - will be determined by actual app connections
GP_PORT_OK="unknown"
HDFS_PORT_OK="unknown"

echo ""

# =============================================================================
# DATABASE CONNECTIVITY TESTS
# =============================================================================
echo "🗄️  Database Connectivity Tests"
echo "=============================================="

# Function to install PostgreSQL client
install_postgresql_client() {
    echo "🔧 Installing PostgreSQL client..."
    
    # Check if Homebrew is available
    if ! command -v brew >/dev/null 2>&1; then
        echo "❌ Homebrew is not installed. Installing Homebrew first..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        
        # Add Homebrew to PATH for this session
        if [[ $(uname -m) == "arm64" ]]; then
            export PATH="/opt/homebrew/bin:$PATH"
        else
            export PATH="/usr/local/bin:$PATH"
        fi
    fi
    
    echo "Installing PostgreSQL client via Homebrew..."
    if brew install libpq > /dev/null 2>&1; then
        echo "✅ PostgreSQL client installed successfully"
        
        # Find where brew actually installed libpq
        LIBPQ_PREFIX=$(brew --prefix libpq 2>/dev/null)
        if [ -n "$LIBPQ_PREFIX" ] && [ -d "$LIBPQ_PREFIX/bin" ]; then
            HOMEBREW_PATH="$LIBPQ_PREFIX/bin"
            echo "Found libpq at: $LIBPQ_PREFIX"
        else
            # Fallback to standard paths
            if [[ $(uname -m) == "arm64" ]]; then
                HOMEBREW_PATH="/opt/homebrew/bin"
            else
                HOMEBREW_PATH="/usr/local/bin"
            fi
        fi
        
        echo "Will add to PATH: $HOMEBREW_PATH"
        
        # Check if PATH is already in .zshrc
        if [ -f ~/.zshrc ]; then
            if ! grep -q "$HOMEBREW_PATH" ~/.zshrc 2>/dev/null; then
                echo "Adding Homebrew path to ~/.zshrc..."
                echo "" >> ~/.zshrc
                echo "# Added by IMC schema setup script" >> ~/.zshrc
                echo "export PATH=\"$HOMEBREW_PATH:\$PATH\"" >> ~/.zshrc
                echo "✅ PATH updated in ~/.zshrc"
            else
                echo "✅ PATH already configured in ~/.zshrc"
            fi
        else
            echo "Creating ~/.zshrc and adding PATH..."
            echo "# Added by IMC schema setup script" > ~/.zshrc
            echo "export PATH=\"$HOMEBREW_PATH:\$PATH\"" >> ~/.zshrc
            echo "✅ ~/.zshrc created with PATH configuration"
        fi
        
        # Update PATH for current session
        export PATH="$HOMEBREW_PATH:$PATH"
        
        # Force reload of shell functions/commands
        hash -r
        
        # Also try sourcing the updated .zshrc for current session
        if [ -f ~/.zshrc ]; then
            source ~/.zshrc 2>/dev/null || true
        fi
        
        # Update PATH again to be sure
        export PATH="$HOMEBREW_PATH:$PATH"
        
        # Verify installation with multiple attempts
        sleep 1  # Brief pause to allow system to update
        
        if command -v psql >/dev/null 2>&1; then
            echo "✅ psql is now available: $(psql --version | head -1)"
            return 0
        elif [ -x "$HOMEBREW_PATH/psql" ]; then
            echo "✅ psql found at $HOMEBREW_PATH/psql: $($HOMEBREW_PATH/psql --version | head -1)"
            return 0
        else
            echo "⚠️  psql installed but not found in expected PATH. Searching for installation..."
            
            # Search for psql in common libpq locations
            local found_psql=""
            local search_paths=(
                "$HOMEBREW_PATH/psql"
                "$LIBPQ_PREFIX/bin/psql"
                "/opt/homebrew/opt/libpq/bin/psql" 
                "/usr/local/opt/libpq/bin/psql"
            )
            
            for search_path in "${search_paths[@]}"; do
                if [ -x "$search_path" ]; then
                    found_psql="$search_path"
                    echo "✅ psql found at $found_psql: $($found_psql --version | head -1)"
                    
                    # Update the PATH in .zshrc with the correct location
                    local correct_path=$(dirname "$found_psql")
                    if [ -f ~/.zshrc ] && ! grep -q "$correct_path" ~/.zshrc; then
                        echo "" >> ~/.zshrc
                        echo "# Corrected libpq path" >> ~/.zshrc  
                        echo "export PATH=\"$correct_path:\$PATH\"" >> ~/.zshrc
                        echo "✅ Updated ~/.zshrc with correct libpq path: $correct_path"
                    fi
                    
                    export PATH="$correct_path:$PATH"
                    return 0
                fi
            done
            
            if [ -z "$found_psql" ]; then
                echo "❌ psql not found after installation. Checking what was installed..."
                brew list libpq 2>/dev/null | grep bin/psql || echo "psql not found in libpq package"
                return 1
            fi
        fi
    else
        echo "❌ Failed to install PostgreSQL client"
        return 1
    fi
}

# Check if psql is available (check both PATH and known locations)
PSQL_AVAILABLE=false

# First check if psql is in PATH
if command -v psql >/dev/null 2>&1; then
    echo "✅ psql is available ($(psql --version | head -1))"
    PSQL_AVAILABLE=true
else
    # Check known libpq installation locations
    KNOWN_PSQL_LOCATIONS=(
        "/opt/homebrew/opt/libpq/bin/psql"
        "/usr/local/opt/libpq/bin/psql"
        "/opt/homebrew/bin/psql"
        "/usr/local/bin/psql"
    )
    
    for psql_path in "${KNOWN_PSQL_LOCATIONS[@]}"; do
        if [ -x "$psql_path" ]; then
            echo "✅ psql found at $psql_path: $($psql_path --version | head -1)"
            PSQL_AVAILABLE=true
            
            # Add to PATH for current session
            export PATH="$(dirname "$psql_path"):$PATH"
            echo "✅ Added $(dirname "$psql_path") to PATH for current session"
            break
        fi
    done
fi

if [ "$PSQL_AVAILABLE" = false ]; then
    echo "❌ psql (PostgreSQL client) is not installed"
    echo ""
    
    # Offer to install automatically
    read -p "Would you like to install PostgreSQL client now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if install_postgresql_client; then
            PSQL_AVAILABLE=true
            echo ""
            echo "🎉 PostgreSQL client installation complete!"
            echo "Continuing with database connectivity tests..."
            echo ""
        else
            echo ""
            echo "❌ Installation failed. Please install manually:"
            echo "   brew install libpq"
            echo "   echo 'export PATH=\"/opt/homebrew/opt/libpq/bin:\$PATH\"' >> ~/.zshrc"
            echo "   source ~/.zshrc"
            PSQL_AVAILABLE=false
            echo ""
        fi
    else
        echo "📋 To install manually later:"
        echo "   brew install libpq"
        echo "   echo 'export PATH=\"/opt/homebrew/opt/libpq/bin:\$PATH\"' >> ~/.zshrc"
        echo "   source ~/.zshrc"
        echo ""
        echo "⚠️  Cannot proceed with database tests without psql"
        PSQL_AVAILABLE=false
        echo ""
    fi
fi

# Function to run psql (handles both PATH and direct execution)
run_psql() {
    if command -v psql >/dev/null 2>&1; then
        psql "$@"
    else
        # Try to find psql in common locations
        local libpq_prefix=$(brew --prefix libpq 2>/dev/null)
        local psql_locations=(
            "$libpq_prefix/bin/psql"
            "/opt/homebrew/opt/libpq/bin/psql"
            "/usr/local/opt/libpq/bin/psql"
            "/opt/homebrew/bin/psql"
            "/usr/local/bin/psql"
        )
        
        for psql_path in "${psql_locations[@]}"; do
            if [ -x "$psql_path" ]; then
                "$psql_path" "$@"
                return $?
            fi
        done
        
        echo "psql not found in any expected location" >&2
        return 1
    fi
}

# Only proceed with database tests if psql is available
if [ "$PSQL_AVAILABLE" = true ]; then
    # Test basic database connection
    echo "Testing connection to default database ($PGDATABASE)..."
    if run_psql -c "SELECT 'Connection successful' as status, version();" > /dev/null 2>&1; then
        echo "✅ Successfully connected to $PGDATABASE"
        GP_PORT_OK=true
    
    # Get database version and system info
    echo "📊 Database Information:"
    run_psql -t -c "SELECT version();" | head -1 | sed 's/^[ \t]*/   /'
    
    # Check if we can create databases
    echo "Testing database creation permissions..."
    TEST_DB="connectivity_test_$(date +%s)"
    if run_psql -c "CREATE DATABASE $TEST_DB;" > /dev/null 2>&1; then
        echo "✅ Database creation permissions verified"
        run_psql -c "DROP DATABASE $TEST_DB;" > /dev/null 2>&1
    else
        echo "⚠️  Cannot create databases (may need superuser privileges)"
    fi
    
else
    echo "❌ Cannot connect to database $PGDATABASE"
    echo "   Please check your credentials and database settings"
    GP_PORT_OK=false
fi

# Test target database (if different from default and exists)
if [ -n "$TARGET_DATABASE" ] && [ "$TARGET_DATABASE" != "$PGDATABASE" ]; then
    echo ""
    echo "Testing connection to target database ($TARGET_DATABASE)..."
    
    # Check if target database exists
    DB_EXISTS=$(run_psql -t -c "SELECT 1 FROM pg_database WHERE datname='$TARGET_DATABASE';" 2>/dev/null | xargs)
    
    if [ "$DB_EXISTS" = "1" ]; then
        # Try connecting to target database
        ORIGINAL_PGDATABASE="$PGDATABASE"
        export PGDATABASE="$TARGET_DATABASE"
        
        if run_psql -c "SELECT 'Target database connection successful' as status;" > /dev/null 2>&1; then
            echo "✅ Successfully connected to target database $TARGET_DATABASE"
            
            # Check for existing tables
            TABLE_COUNT=$(run_psql -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)
            echo "   Found $TABLE_COUNT tables in target database"
            
        else
            echo "❌ Cannot connect to target database $TARGET_DATABASE"
        fi
        
        # Restore original database
        export PGDATABASE="$ORIGINAL_PGDATABASE"
    else
        echo "ℹ️  Target database $TARGET_DATABASE does not exist yet (will be created during setup)"
    fi
fi

else
    echo "Skipping database connectivity tests - psql not available"
fi

echo ""

# =============================================================================
# HDFS CONNECTIVITY TESTS
# =============================================================================
echo "📁 HDFS Connectivity Tests"
echo "=============================================="

# Function to install Hadoop via Homebrew
install_hadoop_client() {
    echo "🔧 Installing Hadoop client via Homebrew..."
    
    # Check if Homebrew is available
    if ! command -v brew >/dev/null 2>&1; then
        echo "❌ Homebrew is not installed. Installing Homebrew first..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        
        # Add Homebrew to PATH for this session
        if [[ $(uname -m) == "arm64" ]]; then
            export PATH="/opt/homebrew/bin:$PATH"
        else
            export PATH="/usr/local/bin:$PATH"
        fi
    fi
    
    echo "Installing Hadoop..."
    if brew install hadoop > /dev/null 2>&1; then
        echo "✅ Hadoop installed successfully"
        
        # Determine correct Homebrew path based on architecture
        if [[ $(uname -m) == "arm64" ]]; then
            HADOOP_PATH="/opt/homebrew/bin"
        else
            HADOOP_PATH="/usr/local/bin"
        fi
        
        # Check if PATH is already in .zshrc
        if [ -f ~/.zshrc ]; then
            if ! grep -q "$HADOOP_PATH" ~/.zshrc 2>/dev/null; then
                echo "Adding Hadoop path to ~/.zshrc..."
                echo "" >> ~/.zshrc
                echo "# Added by IMC schema setup script - Hadoop" >> ~/.zshrc
                echo "export PATH=\"$HADOOP_PATH:\$PATH\"" >> ~/.zshrc
                echo "✅ PATH updated in ~/.zshrc"
            else
                echo "✅ PATH already configured in ~/.zshrc"
            fi
        else
            echo "Creating ~/.zshrc and adding PATH..."
            echo "# Added by IMC schema setup script - Hadoop" > ~/.zshrc
            echo "export PATH=\"$HADOOP_PATH:\$PATH\"" >> ~/.zshrc
            echo "✅ ~/.zshrc created with PATH configuration"
        fi
        
        # Update PATH for current session
        export PATH="$HADOOP_PATH:$PATH"
        
        # Force reload of shell functions/commands
        hash -r
        
        # Verify installation
        if command -v hdfs >/dev/null 2>&1; then
            echo "✅ hdfs is now available: $(hdfs version | head -1)"
            return 0
        else
            echo "⚠️  hdfs installed but not found in PATH. You may need to restart your terminal."
            return 1
        fi
    else
        echo "❌ Failed to install Hadoop"
        return 1
    fi
}

# Check if HDFS client is available
HDFS_CLIENT_AVAILABLE=false
HDFS_CONNECTION_SUCCESS=false

if command -v hdfs >/dev/null 2>&1; then
    echo "✅ HDFS client is available ($(hdfs version | head -1))"
    HDFS_CLIENT_AVAILABLE=true
else
    echo "❌ HDFS client not found"
    echo ""
    
    # Offer to install automatically
    read -p "Would you like to install Hadoop client now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if install_hadoop_client; then
            HDFS_CLIENT_AVAILABLE=true
            echo ""
            echo "🎉 Hadoop client installation complete!"
            echo "Continuing with HDFS connectivity tests..."
            echo ""
        else
            echo ""
            echo "❌ Installation failed. Please install manually:"
            echo "   brew install hadoop"
            HDFS_CLIENT_AVAILABLE=false
            echo ""
        fi
    else
        echo "📋 To install manually later:"
        echo "   brew install hadoop"
        echo ""
        echo "⚠️  Cannot test HDFS connectivity without hadoop client"
        HDFS_CLIENT_AVAILABLE=false
        echo ""
    fi
fi

# Test HDFS connectivity if client is available
if [ "$HDFS_CLIENT_AVAILABLE" = true ]; then
    # Construct HDFS URL
    HDFS_URL="${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
    
    echo "Testing HDFS connectivity..."
    echo "   Connecting to: $HDFS_URL/"
    
    # Test HDFS connectivity with timeout
    echo "   Testing HDFS directory listing with timeout..."
    
    # Use gtimeout with a shorter timeout
    if command -v gtimeout >/dev/null 2>&1; then
        hdfs_output=$(HADOOP_USER_NAME=hdfs gtimeout 5 hdfs dfs -ls "$HDFS_URL/" 2>&1)
        hdfs_exit_code=$?
        
        if [ $hdfs_exit_code -eq 124 ]; then
            echo "❌ HDFS connection timed out after 5 seconds"
            echo "   This usually means the HDFS service is not responding"
            HDFS_CONNECTION_SUCCESS=false
            HDFS_PORT_OK=false
        elif [ $hdfs_exit_code -eq 0 ]; then
            echo "✅ HDFS directory listing successful"
            echo "   Root directory contents:"
            echo "$hdfs_output" | head -10 | sed 's/^/     /'
            
            HDFS_CONNECTION_SUCCESS=true
            HDFS_PORT_OK=true
            
            # Test write permissions
            echo ""
            echo "   Testing HDFS write permissions..."
            TEST_FILE="$HDFS_URL/connectivity-test-$(date +%s).txt"
            
            write_output=$(HADOOP_USER_NAME=hdfs gtimeout 5 hdfs dfs -put /dev/null "$TEST_FILE" 2>&1)
            write_exit_code=$?
            
            if [ $write_exit_code -eq 0 ]; then
                echo "✅ HDFS write permissions verified - created test file"
                echo "   Cleaning up test file..."
                HADOOP_USER_NAME=hdfs gtimeout 3 hdfs dfs -rm "$TEST_FILE" 2>/dev/null
            elif [ $write_exit_code -eq 124 ]; then
                echo "⚠️  HDFS write test timed out"
                echo "   This may indicate permission issues or slow filesystem"
            else
                echo "⚠️  Cannot write to HDFS root directory"
                write_error=$(echo "$write_output" | grep -E "(Permission denied|put:)" | head -1)
                if [ -n "$write_error" ]; then
                    echo "   Write error: $write_error"
                fi
                echo "   This is normal - most HDFS clusters restrict root directory writes"
            fi
            
            echo ""
            echo "Performing additional HDFS tests..."
        else
            echo "❌ HDFS connection failed"
            echo "   Debug: Exit code $hdfs_exit_code"
            echo "   Error details:"
            echo "$hdfs_output" | head -3 | sed 's/^/     /'
            
            # Check for common issues
            if echo "$hdfs_output" | grep -q "Connection refused"; then
                echo "   ➤ Connection refused - HDFS service may not be running on port $HDFS_NAMENODE_PORT"
                echo "   ➤ Check if HDFS NameNode is running on $HDFS_NAMENODE_HOST:$HDFS_NAMENODE_PORT"
            elif echo "$hdfs_output" | grep -q "ConnectException"; then
                echo "   ➤ Network connectivity issue - check firewall settings"
            fi
            
            HDFS_CONNECTION_SUCCESS=false
            HDFS_PORT_OK=false
        fi
    else
        echo "❌ gtimeout not available - cannot test HDFS safely"
        echo "   Install coreutils: brew install coreutils"
        HDFS_CONNECTION_SUCCESS=false
        HDFS_PORT_OK="unknown"
    fi
fi

# If we have a working HDFS connection, do additional tests
if [ "$HDFS_CONNECTION_SUCCESS" = true ]; then
    HDFS_CMD="HADOOP_USER_NAME=hdfs hdfs dfs"
    
    # Check telemetry data paths
    echo "Checking telemetry data paths..."
    
    # Construct telemetry path
    TELEMETRY_PATH="$HDFS_URL/insurance-megacorp/telemetry-data"
    
    if $HDFS_CMD -test -d "$TELEMETRY_PATH" 2>/dev/null; then
        echo "✅ Telemetry data directory exists: $TELEMETRY_PATH"
        
        # Count files/directories in telemetry path
        FILE_COUNT=$($HDFS_CMD -ls "$TELEMETRY_PATH" 2>/dev/null | wc -l)
        if [ "$FILE_COUNT" -gt 1 ]; then
            echo "   Found $((FILE_COUNT - 1)) items in telemetry directory"
        else
            echo "   Telemetry directory is empty"
        fi
        
    else
        echo "ℹ️  Telemetry data directory does not exist yet: $TELEMETRY_PATH"
        echo "   This is normal if the crash detection app hasn't started writing data"
    fi
    
    # Test write permissions (create/delete test directory)
    TEST_DIR="$HDFS_URL/connectivity-test-$(date +%s)"
    if $HDFS_CMD -mkdir "$TEST_DIR" 2>/dev/null; then
        echo "✅ HDFS write permissions verified"
        $HDFS_CMD -rmdir "$TEST_DIR" 2>/dev/null
    else
        echo "⚠️  Cannot write to HDFS (may need proper permissions)"
    fi
    
else
    echo ""
    echo "⚠️  HDFS connectivity could not be established"
    echo "   External tables will be tested on the Greenplum cluster"
    echo "   To install HDFS client manually:"
    echo "   - Install Hadoop: brew install hadoop"
fi

echo ""

# =============================================================================
# PXF CONNECTIVITY TESTS (if applicable)
# =============================================================================
echo "🔗 PXF Connectivity Tests"
echo "=============================================="

# Test if PXF command is available locally
if command -v pxf >/dev/null 2>&1; then
    echo "✅ PXF client is available locally"
    
    # Try to get PXF status
    if pxf status > /dev/null 2>&1; then
        echo "✅ PXF service is running"
    else
        echo "⚠️  PXF service status unknown or not running locally"
    fi
else
    echo "ℹ️  PXF client not available locally"
    echo "   PXF will be tested through Greenplum external tables"
fi

echo ""

# =============================================================================
# DNS RESOLUTION TESTS
# =============================================================================
echo "🌍 DNS Resolution Tests"
echo "=============================================="

# Test DNS resolution for hosts
echo "Testing DNS resolution..."

for host in "$PGHOST" "$HDFS_NAMENODE_HOST"; do
    if nslookup "$host" > /dev/null 2>&1; then
        IP=$(nslookup "$host" 2>/dev/null | awk '/^Address: / { print $2 }' | tail -1)
        echo "✅ $host resolves to $IP"
    else
        echo "❌ Cannot resolve $host"
    fi
done

echo ""

# =============================================================================
# SUMMARY AND RECOMMENDATIONS
# =============================================================================
echo "📋 Test Summary and Recommendations"
echo "=============================================="

echo "Test completed at: $(date)"
echo ""

echo "✅ Configuration validation: Passed"

# Network connectivity summary based on ping and actual app connectivity
if ping -c 1 -W 2 "$PGHOST" > /dev/null 2>&1; then
    NET_STATUS="Hosts reachable (application connectivity tested separately)"
else
    NET_STATUS="Failed - hosts not reachable"
fi
echo "🌐 Network connectivity: $NET_STATUS"

# Debug the database connectivity logic
if [ "$PSQL_AVAILABLE" = true ] && run_psql -c "SELECT 1;" > /dev/null 2>&1; then
    DB_STATUS="Passed"
elif [ "$PSQL_AVAILABLE" = false ]; then
    DB_STATUS="psql not installed"
else
    DB_STATUS="Failed"
fi
echo "🗄️  Database connectivity: $DB_STATUS"

echo "📁 HDFS connectivity: $([ "$HDFS_CONNECTION_SUCCESS" = true ] && echo "Passed" || echo "Needs verification")"
echo "🔧 HDFS client: $([ "$HDFS_CLIENT_AVAILABLE" = true ] && echo "Available" || echo "Not available")"

echo ""
echo "📋 Next Steps:"

# Check if basic connectivity is working
if [ "$PSQL_AVAILABLE" = false ]; then
    echo "1. ❌ PostgreSQL client not available"
    echo "   Re-run this script and choose 'y' when prompted to install"
    echo "   # or install manually:"
    echo "   brew install libpq"
    echo ""
    echo "2. 🔄 Re-run connectivity test:"
    echo "   ./test_connectivity.sh $ENVIRONMENT"
elif [ "$PSQL_AVAILABLE" = true ] && run_psql -c "SELECT 1;" > /dev/null 2>&1; then
    echo "1. ✅ Ready to run database setup:"
    echo "   ./setup_remote_database.sh $ENVIRONMENT"
    echo ""
    echo "2. 📊 To test external tables after setup:"
    echo "   ./test_external_tables.sh"
    echo ""
    echo "3. 🔗 To connect interactively:"
    echo "   ./connect_remote.sh"
    echo ""
    if [ "$HDFS_CONNECTION_SUCCESS" = true ]; then
        echo "4. 🎉 Everything is ready! Both database and HDFS connectivity verified."
    else
        echo "4. ⚠️  HDFS connectivity needs verification on Greenplum cluster via PXF"
    fi
elif [ "$PSQL_AVAILABLE" = true ]; then
    echo "1. ❌ Fix database connectivity issues:"
    echo "   - Verify host: $PGHOST"
    echo "   - Verify port: $PGPORT"
    echo "   - Verify credentials: $PGUSER"
    echo "   - Check network connectivity and firewall settings"
    echo ""
    echo "2. 🔧 Debug connection:"
    echo "   psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE"
fi

echo ""
echo "💡 For detailed logs, check the output above for specific error messages."