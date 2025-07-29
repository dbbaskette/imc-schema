#!/bin/bash
set -e

# This script configures and runs the database setup for a remote instance.
#
# IMPORTANT:
# Replace the placeholder values below with your actual remote database credentials.
#
# Make this script executable before running:
# chmod +x run_on_remote.sh

# --- BEGIN CONFIGURATION ---

# Your remote database server's hostname or IP address
REMOTE_HOST="your_remote_host"

# The port your remote database is listening on (e.g., 5432 for PostgreSQL)
REMOTE_PORT="5432"

# The name of the database you want to set up
DATABASE_NAME="your_database_name"

# The username for connecting to the database
DATABASE_USER="your_username"

# The password for the database user
# It is recommended to use a more secure method for handling passwords,
# such as environment variables or a secrets management tool.
DATABASE_PASSWORD="your_password"

# --- END CONFIGURATION ---

SCRIPT_DIR=$(dirname "$0")

# Call the main setup script with the configured remote credentials
"$SCRIPT_DIR/setup_database.sh" "$REMOTE_HOST" "$REMOTE_PORT" "$DATABASE_NAME" "$DATABASE_USER" "$DATABASE_PASSWORD"
