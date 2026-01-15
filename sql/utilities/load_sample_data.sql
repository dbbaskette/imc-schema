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

-- Load sample data (INSERT statements only - tables already created)
\echo 'Loading sample data...'

-- Note: Since table creation and data are in the same files,
-- we need to extract just the INSERT statements or skip this
-- if tables are already created with data.

-- Check if data already exists to avoid duplicates
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM customers) = 0 THEN
        RAISE NOTICE 'Loading sample data for the first time...';
        -- Sample data loading will be handled by the individual table files
        -- during schema creation, so this is a placeholder for future use
    ELSE
        RAISE NOTICE 'Sample data already exists, skipping data load...';
    END IF;
END
$$;

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