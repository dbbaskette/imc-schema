#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# This is the master script to set up the database.
# It creates the schema and loads the sample data.

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

echo "--- Starting Database Setup for '$DB_NAME' at '$DB_HOST:$DB_PORT' ---"

echo ""
"$SCRIPT_DIR/create_schema.sh" "$DB_HOST" "$DB_PORT" "$DB_NAME" "$DB_USER" "$DB_PASSWORD"

echo ""
echo "--- Database Setup Complete ---"

echo ""
echo "To connect to the database, run:"
echo "PGPASSWORD=<password> psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER"
echo ""
echo "To drop all tables, run:"
echo "PGPASSWORD=<password> psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -f drop_tables.sql"
echo ""
