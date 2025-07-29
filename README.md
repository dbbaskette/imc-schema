# Insurance MegaCorp - Database Schema

This directory contains the complete SQL schema and sample data for the Insurance MegaCorp (IMC) demo application. The scripts are designed for Greenplum and PostgreSQL databases.

## Prerequisites

-   `psql` (PostgreSQL command-line client) must be installed and in your system's PATH.
-   A running PostgreSQL or Greenplum database instance.
-   A database user with permissions to create tables and insert data.

## Directory Structure

The project is organized into a modular structure for easy maintenance and extension:

-   `schema/`: Contains individual `.sql` files, each defining the `CREATE TABLE` statement for a single database table.
-   `data/`: Contains individual `.sql` files, each holding the `INSERT` statements to populate a single table with realistic sample data.
-   `create_schema.sql`: A master `psql` script that executes all files in the `schema/` directory in the correct order to build the database structure.
-   `load_sample_data.sql`: A master `psql` script that executes all files in the `data/` directory to populate the tables.
-   `drop_tables.sql`: A utility script to safely drop all tables in the correct dependency order.
-   `*.sh`: Convenience shell scripts for automating the setup process.

## Quick Setup

The easiest way to create and populate the database is to use the master setup script.

1.  **Make scripts executable:**
    From the `imc-schema` directory, run:
    ```sh
    chmod +x *.sh
    ```

2.  **Run the setup script:**
    Provide your target database name as an argument.
    ```sh
    ./setup_database.sh your_database_name
    ```
    Alternatively, you can set the `PGDATABASE` environment variable:
    ```sh
    export PGDATABASE=your_database_name
    ./setup_database.sh
    ```

This script will first drop any existing tables to ensure a clean state, then create the new schema, and finally load all the sample data.

## Manual Setup

If you prefer to run the steps individually, you can use the separate shell scripts to create the schema and load the data. This is useful if you want to reset the data without altering the schema.

```sh
# 1. Create the schema
./create_schema.sh your_database_name

# 2. Load the sample data
./load_sample_data.sh your_database_name
```