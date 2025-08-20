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

## 🛡️ **NEW: Advanced Safe Driver Scoring System**

**[📊 View Complete Safe Driver Documentation →](SAFE_DRIVER_ML_SYSTEM.md)**

Our production **MADlib Machine Learning** system analyzes real-time telemetry data to generate predictive safety scores:

- 🧠 **MADlib Logistic Regression** trained on driver behavior patterns
- 📊 **Real-time Scoring** from telemetry data (speed, g-force, phone usage)
- 🎯 **Risk Categories** from Excellent to High-Risk with intervention triggers
- 📈 **Production APIs** for dashboard integration
- ⚡ **93.4% Accuracy** in identifying accident-prone drivers

**Current Results**: 15 drivers analyzed, ranging from 93.89 (excellent) to 57.83 (high-risk)

---

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

This schema includes PXF external tables for reading telemetry data from HDFS and internal tables for real-time data writes from applications.

### 🧩 Tables Overview

**🛡️ ML-Powered Tables:**
-   **`safe_driver_scores`**: MADlib-generated safety scores (0-100) with risk categories and ML model insights

**External Tables (HDFS Reads):**
-   **`vehicle_telemetry_data_v2`**: New flattened vehicle sensor data (recommended for new applications)
-   **`vehicle_telemetry_data`**: Legacy vehicle sensor data (original nested format)
-   **`crash_reports_data`**: Processed crash reports with risk analysis and emergency response recommendations

**Internal Tables (Application Writes):**
-   **`vehicle_events`**: Real-time telemetry data sink optimized for JDBC streaming

### ✅ Prerequisites for Telemetry Features

1.  **PXF Service**: Greenplum PXF must be running and configured
2.  **HDFS Access**: Connection to HDFS namenode (default: `hdfs://namenode:9000`)
3.  **Telemetry Data**: The imc-crash-detection application must be writing data to HDFS

### 🗂️ Telemetry Data Structure

#### **Current Format (v2) - Recommended**
**Source Path**: `/insurance-megacorp/telemetry-data-v2/`
**Format**: Parquet with Snappy compression  
**Partitioning**: `/YYYY-MM-DD/driver_id=XXX/telemetry-*.parquet`

**Optimized Flattened JSON Schema:**
```json
{
  "policy_id": 200018,
  "vehicle_id": 300021,
  "vin": "1HGBH41JXMN109186",
  "event_time": "2024-01-15T10:30:45.123Z",
  "speed_mph": 32.5,
  "speed_limit_mph": 35,
  "current_street": "Peachtree Street",
  "g_force": 1.18,
  "driver_id": 400018,
  
  "gps_latitude": 33.7701,
  "gps_longitude": -84.3876,
  "gps_altitude": 351.59,
  "gps_speed": 14.5,
  "gps_bearing": 148.37,
  "gps_accuracy": 2.64,
  "gps_satellite_count": 11,
  "gps_fix_time": 150,
  
  "accelerometer_x": 0.1234,
  "accelerometer_y": -0.0567,
  "accelerometer_z": 0.9876,
  
  "gyroscope_x": 0.02,
  "gyroscope_y": -0.01,
  "gyroscope_z": 0.15,
  
  "magnetometer_x": 25.74,
  "magnetometer_y": -8.73,
  "magnetometer_z": 40.51,
  "magnetometer_heading": 148.37,
  
  "barometric_pressure": 1013.25,
  
  "device_battery_level": 82,
  "device_signal_strength": -63,
  "device_orientation": "portrait",
  "device_screen_on": false,
  "device_charging": true
}
```

#### **Legacy Format (v1)**
**Source Path**: `/insurance-megacorp/telemetry-data/`  
**Partitioning**: `policy_id=XXX/year=YYYY/month=MM/date=YYYY-MM-DD/`

> **✅ Schema Changes**: All field names now use consistent flat naming without prefixes for optimal performance. Driver ID is now INTEGER type for better joins and indexing.

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

The schema includes analytical views for telemetry data:

-   **`v_vehicle_telemetry_enriched`**: Telemetry data joined with customer and vehicle information
-   **`v_high_gforce_events`**: Potential crash events based on G-force thresholds (severity classification)
-   **`v_vehicle_behavior_summary`**: Daily behavior summaries by vehicle with metrics
-   **`vehicle_events_view`**: Basic view of vehicle events with calculated fields

### 🔧 Recent Schema Updates

**✅ Version 2.0 (Current) - Optimized Flat Schema:**
- 🚀 **Performance**: Removed field prefixes for optimal query performance
- 🔧 **Data Types**: `driver_id` changed to `INTEGER` for better indexing and joins
- 📊 **Field Names**: Simplified naming: `gps_speed` (not `gps_speed_ms`), `gyroscope_x/y/z` (not `pitch/roll/yaw`)
- 🗂️ **Partitioning**: New date-based partitioning `/YYYY-MM-DD/driver_id=*/` for improved query pruning
- 📈 **Tables**: All internal and external tables updated with consistent schema
- 🔍 **Views**: Analytical views rebuilt to use new field structure
- ✅ **Status**: Database successfully updated and ready for production