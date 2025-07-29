-- =============================================================================
-- Drop All Tables
-- =============================================================================
-- This script drops all tables in the correct order to avoid foreign key
-- constraint issues. It's intended to be called by other master scripts.
-- =============================================================================

DROP TABLE IF EXISTS claims;
DROP TABLE IF EXISTS accidents;
DROP TABLE IF EXISTS driver_safety_scores;
DROP TABLE IF EXISTS drivers;
DROP TABLE IF EXISTS vehicles;
DROP TABLE IF EXISTS policies;
DROP TABLE IF EXISTS customers;