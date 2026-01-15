-- =============================================================================
-- Drop All Tables
-- =============================================================================
-- This script drops all tables in the correct order to avoid foreign key
-- constraint issues. It's intended to be called by other master scripts.
-- =============================================================================

-- Drop views first (depend on external tables)
DROP VIEW IF EXISTS v_crash_hotspots;
DROP VIEW IF EXISTS v_crash_patterns;
DROP VIEW IF EXISTS v_emergency_response_queue;
DROP VIEW IF EXISTS v_crash_reports_enriched;
DROP VIEW IF EXISTS v_vehicle_behavior_summary;
DROP VIEW IF EXISTS v_high_gforce_events;
DROP VIEW IF EXISTS v_vehicle_telemetry_enriched;

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS crash_reports_data;
DROP EXTERNAL TABLE IF EXISTS vehicle_telemetry_data;

-- Drop regular tables
DROP TABLE IF EXISTS claims;
DROP TABLE IF EXISTS accidents;
DROP TABLE IF EXISTS safe_driver_scores;
DROP TABLE IF EXISTS drivers;
DROP TABLE IF EXISTS vehicles;
DROP TABLE IF EXISTS policies;
DROP TABLE IF EXISTS customers;