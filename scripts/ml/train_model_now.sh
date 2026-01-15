#!/bin/bash

# Force train the ML model right now

export PGHOST="big-data-001.kuhn-labs.com"
export PGPORT="5432"
export PGUSER="gpadmin"
export PGDATABASE="insurance_megacorp"

echo "Training ML model..."

# Use the password from the refresh script environment
psql << 'SQL'
-- Drop and retrain model
DROP TABLE IF EXISTS driver_accident_model CASCADE;
DROP TABLE IF EXISTS driver_accident_model_summary CASCADE;

SELECT madlib.logregr_train(
    'driver_ml_training_data',
    'driver_accident_model', 
    'has_accident',
    'ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]',
    NULL,
    15,
    'irls'
);

SELECT 'Model training complete!' as status;
SQL

echo "Done!"
