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

SCRIPT_DIR=$(dirname "$0")

# Load configuration
if [ -f "$SCRIPT_DIR/config.env" ]; then
    source "$SCRIPT_DIR/config.env"
else
    echo "❌ Error: config.env not found!"
    echo "Please copy config.env.example to config.env and update with your settings."
    exit 1
fi

# Display connection info
echo "Connecting to: $PGUSER@$PGHOST:$PGPORT/$PGDATABASE"
echo "Environment: $ENVIRONMENT"
echo ""

# Check if arguments were provided
if [ $# -eq 0 ]; then
    # Interactive mode
    echo "Starting interactive psql session..."
    echo "Type \\q to exit"
    echo ""
    psql
else
    # Command mode - pass all arguments to psql
    psql "$@"
fi