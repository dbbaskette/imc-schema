<div align="center">

<img alt="Insurance MegaCorp" src="assets/imc-schema-banner.png" width="40%" />

<h2>Insurance MegaCorp - Telemetry & Claims Schema</h2>
<p>
  <strong>Greenplum + PXF + HDFS</strong> - Real-time telematics and crash analytics
</p>

</div>

This repository contains the complete SQL schema, ML pipelines, and operational scripts for the Insurance MegaCorp (IMC) demo application. Designed for Greenplum databases with PXF external tables and HDFS integration.

## Prerequisites

- `psql` (PostgreSQL command-line client) in your system's PATH
- A running Greenplum database instance with PXF enabled
- HDFS cluster access for telemetry data
- Python 3.7+ for consolidation tools (optional)

## Repository Structure

```
imc-schema/
├── config.env                    # Configuration (copy from config/config.env.example)
├── README.md                     # This file
│
├── docs/                         # Documentation
│   ├── SAFE_DRIVER_ML_SYSTEM.md  # ML system documentation
│   ├── REFRESH_SCRIPT_USAGE.md   # Score refresh guide
│   ├── PARQUET_CONSOLIDATION.md  # Data consolidation guide
│   ├── json-schema.md            # Telemetry JSON schema
│   └── archive/                  # Historical/deprecated docs
│
├── sql/                          # SQL scripts
│   ├── core/                     # Base table definitions
│   │   ├── customers.sql
│   │   ├── policies.sql
│   │   ├── vehicles.sql
│   │   ├── drivers.sql
│   │   ├── accidents.sql
│   │   └── claims.sql
│   ├── ml/                       # Machine learning tables
│   │   ├── safe_driver_scores.sql
│   │   └── recalculate_safe_driver_scores.sql
│   ├── external/                 # PXF external tables
│   │   └── recreate_vehicle_events.sql
│   ├── views/                    # Analytical views
│   │   └── telemetry_views.sql
│   ├── utilities/                # Utility scripts
│   │   ├── create_schema.sql     # Master schema creation
│   │   ├── drop_tables.sql       # Drop all tables
│   │   ├── load_sample_data.sql  # Sample data
│   │   ├── sample_telemetry_queries.sql
│   │   └── fix_corrupted_telemetry.sql
│   └── archive/                  # Legacy SQL (v1 tables)
│
├── scripts/                      # Shell scripts
│   ├── setup/                    # Database setup
│   │   ├── setup_remote_database.sh  # Primary setup script
│   │   ├── create_schema.sh
│   │   ├── generate_external_tables.sh
│   │   └── setup_consolidation_env.sh
│   ├── ml/                       # ML operations
│   │   ├── refresh_safe_driver_scores.sh      # Main refresh
│   │   ├── refresh_safe_driver_scores_fixed.sh # Corrupted data workaround
│   │   └── train_model_now.sh
│   ├── consolidation/            # Telemetry data management
│   │   ├── cleanup_telemetry_files.sh
│   │   ├── count_telemetry_files.sh
│   │   ├── run_consolidation.sh
│   │   └── preview_consolidation.sh
│   ├── utilities/                # General utilities
│   │   ├── connect_remote.sh
│   │   ├── test_external_tables.sh
│   │   ├── test_connectivity.sh
│   │   └── run_on_remote.sh
│   └── archive/                  # Deprecated scripts
│
├── python/                       # Python tools
│   ├── parquet_consolidator.py   # Main consolidation engine
│   ├── consolidate_remote.py     # SSH-based consolidation
│   ├── demo_consolidation.py     # Demo/testing
│   └── requirements-consolidation.txt
│
├── config/                       # Configuration templates
│   └── config.env.example
│
├── archive/                      # Archived/deprecated files
│
└── logs/                         # Runtime logs (gitignored)
```

## Quick Start

### 1. Configure Connection

```bash
cp config/config.env.example config.env
# Edit config.env with your database and HDFS settings
```

### 2. Setup Database

```bash
chmod +x scripts/**/*.sh
./scripts/setup/setup_remote_database.sh
```

This will:
- Validate configuration
- Test database and HDFS connectivity
- Create schema with external tables
- Load sample data

### 3. Test External Tables

```bash
./scripts/utilities/test_external_tables.sh
```

## Safe Driver ML System

Our production **MADlib Machine Learning** system analyzes telemetry data to generate predictive safety scores.

**[View Complete Documentation](docs/SAFE_DRIVER_ML_SYSTEM.md)**

### Refresh Scores

```bash
# Standard refresh (checks for new data)
./scripts/ml/refresh_safe_driver_scores.sh

# Force model retraining
./scripts/ml/refresh_safe_driver_scores.sh --force
```

### Features
- MADlib Logistic Regression trained on driver behavior
- Real-time scoring from telemetry (speed, g-force, phone usage)
- Risk categories: Excellent, Good, Average, Poor, High-Risk
- 93.4% accuracy in identifying accident-prone drivers

## Telemetry Data Management

### Count Files by Date

```bash
./scripts/consolidation/count_telemetry_files.sh
./scripts/consolidation/count_telemetry_files.sh --month 2025-12
./scripts/consolidation/count_telemetry_files.sh --year 2025
```

### Consolidate Parquet Files

Merge many small files into larger ones for better performance:

```bash
# Preview what would happen
./scripts/consolidation/run_consolidation.sh --date 2025-12-15 --dry-run

# Execute consolidation
./scripts/consolidation/run_consolidation.sh --date 2025-12-15
```

### Cleanup Corrupted Files

Remove tiny/corrupted files (<=1KB):

```bash
./scripts/consolidation/cleanup_telemetry_files.sh
```

## Database Connection

```bash
# Interactive session
./scripts/utilities/connect_remote.sh

# Execute SQL file
./scripts/utilities/connect_remote.sh -f sql/utilities/sample_telemetry_queries.sql

# Single command
./scripts/utilities/connect_remote.sh -c "SELECT COUNT(*) FROM vehicle_telemetry_data_v2;"
```

## Tables Overview

### ML Tables
- **`safe_driver_scores`**: MADlib-generated safety scores (0-100)
- **`driver_behavior_features`**: Extracted behavioral metrics
- **`driver_accident_model`**: Trained logistic regression model

### External Tables (HDFS)
- **`vehicle_telemetry_data_v2`**: Current flattened telemetry format (recommended)
- **`vehicle_telemetry_data`**: Legacy nested format
- **`crash_reports_data`**: Processed crash reports

### Core Tables
- **`customers`**, **`policies`**, **`vehicles`**, **`drivers`**
- **`accidents`**, **`claims`**

## Data Formats

### Telemetry v2 (Current)
- **Path**: `/insurance-megacorp/telemetry-data-v2/`
- **Format**: Parquet with Snappy compression
- **Partitioning**: `/date=YYYY-MM-DD/telemetry-*.parquet`

### Telemetry v1 (Legacy)
- **Path**: `/insurance-megacorp/telemetry-data/`
- **Partitioning**: `policy_id=XXX/year=YYYY/month=MM/date=YYYY-MM-DD/`

## Version History

- **v2.1.0**: Added --force flag for ML refresh, fixed ROUND() casting issues
- **v2.0.0**: Flattened telemetry schema, optimized partitioning
- **v1.1.0**: Safe Driver Scoring system update
- **v1.0.0**: Initial release
