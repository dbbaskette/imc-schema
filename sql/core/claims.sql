-- =============================================================================
-- Claims table definition and sample data
-- =============================================================================

-- Create claims table
CREATE TABLE claims (
    claim_id SERIAL PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES accidents(accident_id),
    claim_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'DENIED')),
    amount NUMERIC(10, 2),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data for claims table
-- This data corresponds to the new accidents in Georgia.

INSERT INTO claims (claim_id, accident_id, claim_date, status, amount, description)
VALUES
-- Claim for Accident 1 (Sarah Chen)
(600001, 500001, '2023-10-15', 'APPROVED', 3250.50, 'Repairs to rear bumper and trunk.'),

-- Claim for Accident 2 (Michael Harris)
(600002, 500002, '2023-11-21', 'DENIED', 800.00, 'Damage estimate is below the policy deductible of $1000.'),

-- Claim for Accident 3 (Carlos Rodriguez - additional driver)
(600003, 500003, '2023-12-01', 'PENDING', 12500.00, 'Awaiting police report and liability assessment for side-impact.'),

-- Claim for Accident 5 (Daniel Harris)
(600004, 500005, '2024-02-05', 'PENDING', 25000.00, 'Major front-end damage. Vehicle is likely a total loss. Awaiting adjuster report.'),

-- Claim for Accident 6 (Zoey Phillips)
(600005, 500006, '2024-02-28', 'APPROVED', 45000.00, 'Vehicle declared a total loss due to rollover. Payout for vehicle value.'),

-- Claim for Accident 8 (Jack Moore)
(600006, 500008, '2024-03-11', 'DENIED', 9500.00, 'Claim denied; driver found to be at fault for the collision.'),

-- Claim for Accident 9 (Liam Smith)
(600007, 500009, '2024-03-13', 'APPROVED', 1250.00, 'Repair to minor bumper damage from parking lot incident.');