-- =============================================================================
-- IMC - Master Sample Data Loading Script
-- =============================================================================
-- This script orchestrates the loading of all sample data into the tables.
-- It should be run using psql after the schema has been created.
--
-- Usage:
-- psql -d your_database_name -f load_sample_data.sql
-- =============================================================================

-- Start a transaction to ensure all data is loaded or none is.
\set ON_ERROR_STOP on
BEGIN;

-- Load data in dependency order
\echo 'Loading sample data...'
\i data/customers.sql
\i data/policies.sql
\i data/vehicles.sql
\i data/drivers.sql
\i data/accidents.sql
\i data/claims.sql

-- =============================================================================
-- Reset sequences to a higher number to avoid conflicts with manual inserts
-- =============================================================================
-- This is good practice in a demo environment. It prevents the next manually
-- inserted row from conflicting with the hardcoded IDs in the sample data.
\echo 'Resetting table sequences...'
DO $$
DECLARE
    max_id INTEGER;
BEGIN
    SELECT COALESCE(MAX(customer_id), 0) INTO max_id FROM customers;
    PERFORM setval('customers_customer_id_seq', max_id + 100, true);

    SELECT COALESCE(MAX(policy_id), 0) INTO max_id FROM policies;
    PERFORM setval('policies_policy_id_seq', max_id + 100, true);

    SELECT COALESCE(MAX(vehicle_id), 0) INTO max_id FROM vehicles;
    PERFORM setval('vehicles_vehicle_id_seq', max_id + 100, true);

    SELECT COALESCE(MAX(driver_id), 0) INTO max_id FROM drivers;
    PERFORM setval('drivers_driver_id_seq', max_id + 100, true);

    SELECT COALESCE(MAX(accident_id), 0) INTO max_id FROM accidents;
    PERFORM setval('accidents_accident_id_seq', max_id + 100, true);

    SELECT COALESCE(MAX(claim_id), 0) INTO max_id FROM claims;
    PERFORM setval('claims_claim_id_seq', max_id + 100, true);
END $$;

COMMIT;
\echo 'Sample data loading complete.'