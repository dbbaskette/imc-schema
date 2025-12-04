#!/bin/bash
# =============================================================================
# Safe Driver Score Refresh Script (Fixed for Corrupted Parquet)
# =============================================================================
# Automates the recalculation of safe driver scores when new data arrives
# Uses vehicle_telemetry_data_v2_working to skip corrupted partitions
#
# Usage:
#   ./refresh_safe_driver_scores_fixed.sh [--force]
#
# Options:
#   --force    Force model retraining regardless of day or new accidents
# =============================================================================

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Parse command line arguments
FORCE_RETRAIN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE_RETRAIN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--force]"
            exit 1
            ;;
    esac
done

# Load configuration
source config.env

# Setup logging
LOG_FILE="${LOG_DIR:-./logs}/safe_driver_refresh_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$(dirname "$LOG_FILE")"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Error handling
handle_error() {
    log "ERROR: Script failed at line $1"
    log "ERROR: Command: $BASH_COMMAND"
    exit 1
}

trap 'handle_error $LINENO' ERR

# =============================================================================
# Main Execution
# =============================================================================

log "Starting Safe Driver Score Refresh Process (Using Working Table)"
log "Target Database: $TARGET_DATABASE"
log "Host: $PGHOST:$PGPORT"

# Step 0: Ensure working table exists
log "Step 0: Setting up working table to skip corrupted partitions..."
PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -f fix_corrupted_telemetry.sql >> "$LOG_FILE" 2>&1
log "✅ Working table ready"

# Step 1: Verify database connection
log "Step 1: Verifying database connection..."
PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -c "SELECT COUNT(*) FROM vehicle_telemetry_data_v2_working;" > /dev/null
log "✅ Database connection successful"

# Step 2: Check for new telemetry data
log "Step 2: Checking for new telemetry data..."

# Check if driver_behavior_features table exists and has last_updated column
TABLE_EXISTS=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name = 'driver_behavior_features';
" | xargs)

COLUMN_EXISTS=0
if [ "$TABLE_EXISTS" -eq 1 ]; then
    COLUMN_EXISTS=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = 'driver_behavior_features'
        AND column_name = 'last_updated';
    " | xargs)
fi

if [ "$TABLE_EXISTS" -eq 0 ] || [ "$COLUMN_EXISTS" -eq 0 ]; then
    log "⚠️  driver_behavior_features table/column doesn't exist yet - first time setup"
    log "Will process all telemetry data..."
    NEW_RECORDS=1  # Force processing
else
    # Table and column exist, check for new data since last update
    NEW_RECORDS=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
        SELECT COUNT(*)
        FROM vehicle_telemetry_data_v2_working vtd
        LEFT JOIN driver_behavior_features dbf ON vtd.driver_id = dbf.driver_id
        WHERE vtd.event_time::timestamp > COALESCE(dbf.last_updated, '2024-01-01'::timestamp)
           OR dbf.driver_id IS NULL;
    " | xargs)
    log "Found $NEW_RECORDS new/updated telemetry records"
fi

if [ "$NEW_RECORDS" -eq 0 ]; then
    log "No new data found. Exiting."
    exit 0
fi

# Step 3: Execute score recalculation (using modified SQL)
log "Step 3: Executing score recalculation..."
PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" << 'EOF' >> "$LOG_FILE" 2>&1

-- Use the working table instead of the original
DROP TABLE IF EXISTS driver_behavior_features_new;
CREATE TABLE driver_behavior_features_new AS
SELECT
    driver_id,
    COUNT(*) as total_events,
    ROUND(
        (COUNT(*) - COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph))::NUMERIC / COUNT(*) * 100,
        2
    ) as speed_compliance_rate,
    ROUND(AVG(g_force), 4) as avg_g_force,
    COUNT(*) FILTER (WHERE g_force > 1.5) as harsh_driving_events,
    ROUND(
        COUNT(*) FILTER (WHERE device_screen_on = true AND speed_mph > 5)::NUMERIC / COUNT(*) * 100,
        2
    ) as phone_usage_rate,
    ROUND(STDDEV(speed_mph), 2) as speed_variance,
    ROUND(AVG(speed_mph), 2) as avg_speed,
    ROUND(MAX(speed_mph), 2) as max_speed,
    COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph + 10) as excessive_speeding_count,
    NOW() as last_updated
FROM vehicle_telemetry_data_v2_working
WHERE driver_id IS NOT NULL
GROUP BY driver_id;

DROP TABLE IF EXISTS driver_behavior_features CASCADE;
ALTER TABLE driver_behavior_features_new RENAME TO driver_behavior_features;
ALTER TABLE driver_behavior_features ADD PRIMARY KEY (driver_id);

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
) acc ON dbf.driver_id = acc.driver_id;

DROP TABLE IF EXISTS driver_ml_training_data CASCADE;
ALTER TABLE driver_ml_training_data_new RENAME TO driver_ml_training_data;

DROP TABLE IF EXISTS safe_driver_scores_new;
CREATE TABLE safe_driver_scores_new AS
SELECT
    driver_id,
    madlib.logregr_predict_prob(
        (SELECT coef FROM driver_accident_model),
        ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
    ) as accident_probability,
    ROUND(
        100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2
    ) as safe_driver_score,
    CASE
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 90 THEN 'EXCELLENT'
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 80 THEN 'GOOD'
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 70 THEN 'AVERAGE'
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 60 THEN 'POOR'
        ELSE 'HIGH_RISK'
    END as risk_category,
    total_events,
    speed_compliance_rate,
    avg_g_force,
    harsh_driving_events,
    phone_usage_rate,
    accident_count,
    NOW() as calculation_date
FROM driver_ml_training_data;

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

DROP TABLE IF EXISTS safe_driver_scores_new;

EOF

# Step 4: Verify results
log "Step 4: Verifying recalculation results..."
STATS=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
    SELECT
        COUNT(*) as total_drivers,
        ROUND(AVG(score), 2) as avg_score,
        COUNT(*) FILTER (WHERE score >= 90) as excellent_count,
        COUNT(*) FILTER (WHERE score < 60) as high_risk_count
    FROM v_current_driver_scores;
" | xargs)

log "✅ Recalculation completed successfully"
log "📊 Results: $STATS"

# Step 5: Update model if needed (weekly on Sundays or --force)
if [ "$FORCE_RETRAIN" = true ]; then
    log "Step 5: FORCED model retraining requested..."
    SHOULD_RETRAIN=true
elif [ "$(date +%u)" = "7" ]; then  # Sunday = 7
    log "Step 5: Weekly model retraining (Sunday)..."

    ACCIDENT_CHANGES=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
        SELECT COUNT(*) FROM accidents
        WHERE created_date > (NOW() - INTERVAL '7 days');
    " | xargs)

    if [ "$ACCIDENT_CHANGES" -gt 0 ]; then
        log "Found $ACCIDENT_CHANGES new accidents - retraining model..."
        SHOULD_RETRAIN=true
    else
        log "No new accidents - skipping model retrain"
        SHOULD_RETRAIN=false
    fi
else
    log "Step 5: Skipping model retrain (not Sunday, use --force to override)"
    SHOULD_RETRAIN=false
fi

if [ "$SHOULD_RETRAIN" = true ]; then
    log "🔄 Retraining model..."

    PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -c "
        DROP TABLE IF EXISTS driver_accident_model CASCADE;
        DROP TABLE IF EXISTS driver_accident_model_summary CASCADE;

        SELECT madlib.logregr_train(
            'driver_ml_training_data',
            'driver_accident_model',
            'has_accident',
            'ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]',
            NULL,
            20,
            'irls'
        );
    " >> "$LOG_FILE" 2>&1

    log "✅ Model retrained successfully"
    log "Recalculating scores with updated model..."

    # Re-run the scoring with new model (using inline SQL to avoid recursion)
    PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" << 'RESCORE' >> "$LOG_FILE" 2>&1

DROP TABLE IF EXISTS safe_driver_scores_new;
CREATE TABLE safe_driver_scores_new AS
SELECT
    driver_id,
    madlib.logregr_predict_prob(
        (SELECT coef FROM driver_accident_model),
        ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
    ) as accident_probability,
    ROUND(
        100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2
    ) as safe_driver_score,
    CASE
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 90 THEN 'EXCELLENT'
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 80 THEN 'GOOD'
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 70 THEN 'AVERAGE'
        WHEN ROUND(100 * (1 - madlib.logregr_predict_prob(
            (SELECT coef FROM driver_accident_model),
            ARRAY[1, speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
        )), 2) >= 60 THEN 'POOR'
        ELSE 'HIGH_RISK'
    END as risk_category,
    total_events,
    speed_compliance_rate,
    avg_g_force,
    harsh_driving_events,
    phone_usage_rate,
    accident_count,
    NOW() as calculation_date
FROM driver_ml_training_data;

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
        ' | Model: MADlib Logistic Regression (Retrained)'
    ) as notes
FROM safe_driver_scores_new;

DROP TABLE IF EXISTS safe_driver_scores_new;

RESCORE

    log "✅ Scores recalculated with new model"
fi

# Step 6: Send alerts for high-risk drivers
log "Step 6: Checking for high-risk drivers requiring alerts..."
HIGH_RISK_DRIVERS=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
    SELECT STRING_AGG(driver_id::text, ',')
    FROM v_current_driver_scores
    WHERE score < 60;
" | xargs)

if [ -n "$HIGH_RISK_DRIVERS" ] && [ "$HIGH_RISK_DRIVERS" != "" ]; then
    log "⚠️  HIGH-RISK ALERT: Drivers requiring intervention: $HIGH_RISK_DRIVERS"
else
    log "✅ No high-risk drivers detected"
fi

# Step 7: Performance metrics
log "Step 7: Performance summary..."
PERFORMANCE=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
    WITH score_distribution AS (
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE score >= 90) as excellent,
            COUNT(*) FILTER (WHERE score >= 80 AND score < 90) as good,
            COUNT(*) FILTER (WHERE score >= 70 AND score < 80) as average,
            COUNT(*) FILTER (WHERE score >= 60 AND score < 70) as poor,
            COUNT(*) FILTER (WHERE score < 60) as high_risk
        FROM v_current_driver_scores
    )
    SELECT
        'Total: ' || total ||
        ' | Excellent: ' || excellent ||
        ' | Good: ' || good ||
        ' | Average: ' || average ||
        ' | Poor: ' || poor ||
        ' | High-Risk: ' || high_risk
    FROM score_distribution;
" | xargs)

log "📈 Score Distribution: $PERFORMANCE"

log "🎉 Safe Driver Score Refresh Complete!"
log "📄 Full log: $LOG_FILE"
