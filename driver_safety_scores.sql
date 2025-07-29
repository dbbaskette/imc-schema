-- =============================================================================
-- Table: driver_safety_scores
-- Description: This table will be populated by a separate process.
-- =============================================================================
CREATE TABLE driver_safety_scores (
    score_id SERIAL PRIMARY KEY,
    driver_id INTEGER NOT NULL REFERENCES drivers(driver_id),
    score NUMERIC(5, 2) NOT NULL,
    calculation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    notes TEXT
);