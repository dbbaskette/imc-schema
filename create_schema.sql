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

-- Drop existing tables to ensure a clean slate
\echo 'Dropping existing tables...'
\i drop_tables.sql

-- Create tables in the correct order to satisfy foreign key constraints
\echo 'Creating tables...'
\i schema/customers.sql
\i schema/policies.sql
\i schema/vehicles.sql
\i schema/drivers.sql
\i schema/driver_safety_scores.sql
\i schema/accidents.sql
\i schema/claims.sql

COMMIT;
\echo 'Schema creation complete.'