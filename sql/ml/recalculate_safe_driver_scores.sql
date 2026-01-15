-- =============================================================================
-- Safe Driver Score Recalculation Procedure
-- =============================================================================
-- This script recalculates safe driver scores as new telemetry data arrives
-- Run this script whenever new telemetry data is loaded into the system
-- =============================================================================

-- Step 1: Refresh Driver Behavior Features
-- =============================================================================
-- This extracts the latest behavioral metrics from all telemetry data
-- =============================================================================

DROP TABLE IF EXISTS driver_behavior_features_new;
CREATE TABLE driver_behavior_features_new AS
SELECT
    driver_id,

    -- Volume metrics
    COUNT(*) as total_events,

    -- Speed compliance (primary ML feature - 40% impact)
    ROUND(
        (COUNT(*) - COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph))::NUMERIC / COUNT(*) * 100,
        2
    ) as speed_compliance_rate,

    -- Driving smoothness (secondary ML feature - 25% impact)
    ROUND(AVG(g_force)::NUMERIC, 4) as avg_g_force,
    COUNT(*) FILTER (WHERE g_force > 1.5) as harsh_driving_events,

    -- Distraction indicators (15% impact)
    ROUND(
        COUNT(*) FILTER (WHERE device_screen_on = true AND speed_mph > 5)::NUMERIC / COUNT(*) * 100,
        2
    ) as phone_usage_rate,

    -- Speed consistency (5% impact)
    ROUND(STDDEV(speed_mph)::NUMERIC, 2) as speed_variance,

    -- Additional metrics for analysis
    ROUND(AVG(speed_mph)::NUMERIC, 2) as avg_speed,
    ROUND(MAX(speed_mph)::NUMERIC, 2) as max_speed,
    COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph + 10) as excessive_speeding_count,

    -- Timestamp
    NOW() as last_updated

FROM vehicle_telemetry_data_v2_working
WHERE driver_id IS NOT NULL
GROUP BY driver_id
DISTRIBUTED BY (driver_id);

-- Replace the existing features table
DROP TABLE IF EXISTS driver_behavior_features CASCADE;
ALTER TABLE driver_behavior_features_new RENAME TO driver_behavior_features;

-- Add primary key (table is already distributed by driver_id)
ALTER TABLE driver_behavior_features ADD PRIMARY KEY (driver_id);

-- Step 2: Update Training Data with Accident History
-- =============================================================================
-- Combine behavioral features with accident history for ML model
-- =============================================================================

DROP TABLE IF EXISTS driver_ml_training_data_new;
CREATE TABLE driver_ml_training_data_new AS
SELECT
    dbf.*,
    COALESCE(acc.accident_count, 0) as accident_count,
    CASE WHEN acc.accident_count > 0 THEN 1 ELSE 0 END as has_accident
FROM driver_behavior_features dbf
LEFT JOIN (
    SELECT
        driver_id::INTEGER,
        COUNT(*) as accident_count
    FROM accidents
    GROUP BY driver_id::INTEGER
) acc ON dbf.driver_id = acc.driver_id
DISTRIBUTED BY (driver_id);

-- Replace the existing training data
DROP TABLE IF EXISTS driver_ml_training_data CASCADE;
ALTER TABLE driver_ml_training_data_new RENAME TO driver_ml_training_data;

-- Step 3: Retrain MADlib Model (Optional - only if needed)
-- =============================================================================
-- Uncomment this section if you want to retrain the model with new data
-- Normally you would retrain weekly or when significant new patterns emerge
-- =============================================================================

-- Drop existing model
DROP TABLE IF EXISTS driver_accident_model CASCADE;
DROP TABLE IF EXISTS driver_accident_model_summary CASCADE;

-- Retrain logistic regression model
SELECT madlib.logregr_train(
    'driver_ml_training_data',                    -- Training dataset
    'driver_accident_model',                      -- Output model table
    'has_accident',                               -- Target variable (0/1)
    'ARRAY[1, speed_compliance_rate, avg_g_force,
           harsh_driving_events, phone_usage_rate,
           speed_variance]',                      -- Feature vector with intercept
    NULL,                                         -- No grouping
    20,                                           -- Max iterations
    'irls'                                        -- Iteratively Reweighted Least Squares
);

-- Step 4: Calculate New Safety Scores
-- =============================================================================
-- Generate updated safety scores using the existing trained model
-- =============================================================================

DROP TABLE IF EXISTS safe_driver_scores_new;
CREATE TABLE safe_driver_scores_new AS
SELECT
    driver_id,

    -- Get accident probability from MADlib model
    madlib.logregr_predict_prob(
        (SELECT coef FROM driver_accident_model),
        ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
    ) as accident_probability,

    -- Convert to safety score (0-100 scale)
    ROUND(
        (100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )))::NUMERIC, 2
    ) as safe_driver_score,

    -- Determine risk category
    CASE
        WHEN ROUND((100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )))::NUMERIC, 2) >= 90 THEN 'EXCELLENT'
        WHEN ROUND((100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )))::NUMERIC, 2) >= 80 THEN 'GOOD'
        WHEN ROUND((100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )))::NUMERIC, 2) >= 70 THEN 'AVERAGE'
        WHEN ROUND((100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )))::NUMERIC, 2) >= 60 THEN 'POOR'
        ELSE 'HIGH_RISK'
    END as risk_category,

    -- Include feature details for analysis
    total_events,
    speed_compliance_rate,
    avg_g_force,
    harsh_driving_events,
    phone_usage_rate,
    accident_count,

    NOW() as calculation_date

FROM driver_ml_training_data
DISTRIBUTED BY (driver_id);

-- Step 5: Insert New Scores into Production Table
-- =============================================================================
-- Add new scores to the safe_driver_scores table with history tracking
-- =============================================================================

INSERT INTO safe_driver_scores (driver_id, score, calculation_date, notes)
SELECT 
    driver_id,
    safe_driver_score,
    calculation_date,
    CONCAT(
        'ML Risk Category: ', risk_category, 
        ' | Speed Compliance: ', speed_compliance_rate, '%',
        ' | Harsh Events: ', harsh_driving_events,
        ' | Phone Usage: ', phone_usage_rate, '%',
        ' | Accidents: ', accident_count,
        ' | Model: MADlib Logistic Regression'
    ) as notes
FROM safe_driver_scores_new;

-- Step 6: Create/Update Current Scores View
-- =============================================================================
-- Create a view showing the latest score for each driver
-- =============================================================================

DROP VIEW IF EXISTS v_current_driver_scores CASCADE;
CREATE VIEW v_current_driver_scores AS
SELECT DISTINCT ON (driver_id)
    driver_id,
    score,
    calculation_date,
    notes,
    CASE 
        WHEN score >= 90 THEN 'EXCELLENT'
        WHEN score >= 80 THEN 'GOOD'
        WHEN score >= 70 THEN 'AVERAGE'
        WHEN score >= 60 THEN 'POOR'
        ELSE 'HIGH_RISK'
    END as risk_category
FROM safe_driver_scores
ORDER BY driver_id, calculation_date DESC;

-- Step 7: Performance Analysis
-- =============================================================================
-- Show summary of score changes
-- =============================================================================

SELECT
    'RECALCULATION SUMMARY' as summary_type,
    COUNT(*) as drivers_scored,
    ROUND(AVG(safe_driver_score)::NUMERIC, 2) as avg_score,
    MIN(safe_driver_score) as min_score,
    MAX(safe_driver_score) as max_score,
    COUNT(*) FILTER (WHERE risk_category = 'EXCELLENT') as excellent_drivers,
    COUNT(*) FILTER (WHERE risk_category = 'GOOD') as good_drivers,
    COUNT(*) FILTER (WHERE risk_category = 'AVERAGE') as average_drivers,
    COUNT(*) FILTER (WHERE risk_category = 'POOR') as poor_drivers,
    COUNT(*) FILTER (WHERE risk_category = 'HIGH_RISK') as high_risk_drivers
FROM safe_driver_scores_new;

-- Cleanup temporary tables
DROP TABLE IF EXISTS safe_driver_scores_new;

-- Success message
SELECT 'Safe driver scores successfully recalculated!' as status;