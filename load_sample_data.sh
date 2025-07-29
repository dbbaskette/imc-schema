#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# This script runs the master data loading SQL script.
# It requires connection details for the target database.

# Assign arguments to variables for clarity
DB_HOST=$1
DB_PORT=$2
DB_NAME=$3
DB_USER=$4
DB_PASSWORD=$5

# Check if all required arguments are provided
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ]; then
  echo "Usage: $0 <host> <port> <database_name> <user> <password>"
  exit 1
fi

SCRIPT_DIR=$(dirname "$0")

echo "--> Running data loading script on database '$DB_NAME' at '$DB_HOST:$DB_PORT'..."

# Use PGPASSWORD to avoid interactive password prompt
export PGPASSWORD=$DB_PASSWORD

# Connect to the remote database and execute the data loading script
psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -f "$SCRIPT_DIR/load_sample_data.sql"

# Unset the password variable for security
unset PGPASSWORD
