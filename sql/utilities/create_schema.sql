-- =============================================================================
-- IMC - Master Schema Creation Script
-- =============================================================================
-- This script orchestrates the creation of the entire database schema.
-- It should be run using psql, which can interpret the \i command.
--
-- Usage:
-- psql -d your_database_name -f create_schema.sql
-- =============================================================================

-- Start a transaction
\set ON_ERROR_STOP on
BEGIN;

\c insurance_megacorp

-- Drop existing tables to ensure a clean slate
\echo 'Dropping existing tables...'
\i drop_tables.sql

-- Create tables in the correct order to satisfy foreign key constraints
\echo 'Creating tables...'
\i customers.sql
\i policies.sql
\i vehicles.sql
\i drivers.sql
\i safe_driver_scores.sql
\i accidents.sql
\i claims.sql

-- Enable PXF extension for external table access
\echo 'Enabling PXF extension...'
CREATE EXTENSION IF NOT EXISTS pxf;

-- Create external tables for HDFS telemetry data (requires PXF)
\echo 'Creating external tables for telemetry data...'
\i vehicle_telemetry_data_generated.sql
\i crash_reports_data_generated.sql

-- Create analytical views
\echo 'Creating analytical views...'
\i telemetry_views.sql

COMMIT;
\echo 'Schema creation complete with telemetry external tables.'