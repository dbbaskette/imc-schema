#!/bin/bash

# =============================================================================
# Remote Database Connection Script
# =============================================================================
# This script provides an easy way to connect to the remote Greenplum database
# using the configuration from config.env
#
# Usage: 
#   ./connect_remote.sh                    # Interactive psql session
#   ./connect_remote.sh -f script.sql     # Execute a SQL file
#   ./connect_remote.sh -c "SELECT 1;"    # Execute a single command
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load configuration from repo root
if [ -f "$REPO_ROOT/config.env" ]; then
    source "$REPO_ROOT/config.env"
elif [ -f "$SCRIPT_DIR/config.env" ]; then
    source "$SCRIPT_DIR/config.env"
else
    echo "❌ Error: config.env not found!"
    echo "Please copy config.env.example to config.env and update with your settings."
    exit 1
fi

# Use TARGET_DATABASE if set, otherwise fall back to PGDATABASE
DB_NAME="${TARGET_DATABASE:-$PGDATABASE}"

# Display connection info
echo "Connecting to: $PGUSER@$PGHOST:$PGPORT/$DB_NAME"
echo "Environment: $ENVIRONMENT"
echo ""

# Check if arguments were provided
if [ $# -eq 0 ]; then
    # Interactive mode
    echo "Starting interactive psql session..."
    echo "Type \\q to exit"
    echo ""
    PGDATABASE="$DB_NAME" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER"
else
    # Command mode - pass all arguments to psql
    PGDATABASE="$DB_NAME" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$@"
fi