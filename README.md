<div align="center">

<img alt="Insurance MegaCorp" src="assets/imc-schema-banner.png" width="40%" />

<h2>🛡️ Insurance MegaCorp · Telemetry & Claims Schema</h2>
<p>
  <strong>Greenplum + PXF + HDFS</strong> · Real‑time telematics and crash analytics
</p>

</div>

This directory contains the complete SQL schema and sample data for the Insurance MegaCorp (IMC) demo application. The scripts are designed for Greenplum and PostgreSQL databases.

## ⚙️ Prerequisites

-   `psql` (PostgreSQL command-line client) must be installed and in your system's PATH.
-   A running PostgreSQL or Greenplum database instance.
-   A database user with permissions to create tables and insert data.

## 📁 Directory Structure

The project is organized into a modular structure for easy maintenance and extension:

-   `schema/`: Contains individual `.sql` files, each defining the `CREATE TABLE` statement for a single database table.
-   `data/`: Contains individual `.sql` files, each holding the `INSERT` statements to populate a single table with realistic sample data.
-   `create_schema.sql`: A master `psql` script that executes all files in the `schema/` directory in the correct order to build the database structure.
-   `load_sample_data.sql`: A master `psql` script that executes all files in the `data/` directory to populate the tables.
-   `drop_tables.sql`: A utility script to safely drop all tables in the correct dependency order.
-   `*.sh`: Convenience shell scripts for automating the setup process.

## 🚀 Quick Setup

### 🌐 Remote Database Setup (Recommended)

The recommended approach is to connect directly from your local machine to the remote Greenplum cluster:

1.  **Configure connection settings:**
    ```sh
    cp config.env.example config.env
    # Edit config.env with your actual database and HDFS settings
    ```

2.  **Make scripts executable:**
    ```sh
    chmod +x *.sh
    ```

3.  **Run the remote setup script:**
    ```sh
    ./setup_remote_database.sh [environment]
    ```
    
    This will:
    - Validate your configuration
    - Test database and HDFS connectivity  
    - Create the schema with dynamic external table paths
    - Load sample data
    - Optionally run validation queries

### 💻 Local Database Setup (Legacy)

For local database setup, you can still use the original approach:

```sh
./setup_database.sh your_database_name
```

## 🧰 Manual Setup

If you prefer to run the steps individually, you can use the separate shell scripts to create the schema and load the data. This is useful if you want to reset the data without altering the schema.

```sh
# 1. Create the schema
./create_schema.sh your_database_name

# 2. Load the sample data
./load_sample_data.sh your_database_name
```

## 📡 Telemetry Data Integration

This schema now includes PXF external tables that provide SQL access to vehicle telemetry data stored in HDFS by the crash detection application.

### 🧩 External Tables

-   **`vehicle_telemetry_data`**: Comprehensive vehicle sensor data including GPS, accelerometer, gyroscope, magnetometer, and device health information
-   **`crash_reports_data`**: Processed crash reports with risk analysis and emergency response recommendations

### ✅ Prerequisites for Telemetry Features

1.  **PXF Service**: Greenplum PXF must be running and configured
2.  **HDFS Access**: Connection to HDFS namenode (default: `hdfs://namenode:9000`)
3.  **Telemetry Data**: The imc-crash-detection application must be writing data to HDFS

### 🗂️ Telemetry Data Structure

**Source Path**: `hdfs://namenode:9000/telemetry-data`
**Format**: Parquet with Snappy compression
**Partitioning**: `policy_id=XXX/year=YYYY/month=MM/date=YYYY-MM-DD/`

### 🔌 Remote Connection and Queries

**Connect to remote database:**
```bash
./connect_remote.sh                    # Interactive session
./connect_remote.sh -f script.sql     # Execute SQL file
./connect_remote.sh -c "SELECT 1;"    # Single command
```

**Test external tables:**
```bash
./test_external_tables.sh             # Comprehensive connectivity test
```

**Run sample telemetry queries:**
```bash
./connect_remote.sh -f sample_telemetry_queries.sql
```

### 📊 Views and Analytics

The schema includes several analytical views:

-   **`v_vehicle_telemetry_enriched`**: Telemetry data joined with customer and vehicle information
-   **`v_high_gforce_events`**: Potential crash events based on G-force thresholds
-   **`v_vehicle_behavior_summary`**: Daily behavior summaries by vehicle
-   **`v_crash_reports_enriched`**: Crash reports with customer context
-   **`v_emergency_response_queue`**: Prioritized list of crashes requiring immediate response
-   **`v_crash_patterns`**: Crash type and severity analysis
-   **`v_crash_hotspots`**: Geographic clustering of crash events