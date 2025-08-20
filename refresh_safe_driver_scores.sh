#!/bin/bash
# =============================================================================
# Safe Driver Score Refresh Script
# =============================================================================
# Automates the recalculation of safe driver scores when new data arrives
# Can be run manually or scheduled via cron
# =============================================================================

set -euo pipefail  # Exit on error, undefined vars, pipe failures

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

log "Starting Safe Driver Score Refresh Process"
log "Target Database: $TARGET_DATABASE"
log "Host: $PGHOST:$PGPORT"

# Step 1: Verify database connection
log "Step 1: Verifying database connection..."
PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -c "SELECT COUNT(*) FROM vehicle_telemetry_data_v2;" > /dev/null
log "✅ Database connection successful"

# Step 2: Check for new telemetry data
log "Step 2: Checking for new telemetry data..."
NEW_RECORDS=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
    SELECT COUNT(*) 
    FROM vehicle_telemetry_data_v2 vtd
    LEFT JOIN driver_behavior_features dbf ON vtd.driver_id = dbf.driver_id
    WHERE vtd.event_time > COALESCE(dbf.last_updated, '2024-01-01'::timestamp)
       OR dbf.driver_id IS NULL;
" | xargs)

log "Found $NEW_RECORDS new/updated telemetry records"

if [ "$NEW_RECORDS" -eq 0 ]; then
    log "No new data found. Exiting."
    exit 0
fi

# Step 3: Execute score recalculation
log "Step 3: Executing score recalculation..."
PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -f recalculate_safe_driver_scores.sql >> "$LOG_FILE" 2>&1

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

# Step 5: Update model if needed (weekly on Sundays)
if [ "$(date +%u)" = "7" ]; then  # Sunday = 7
    log "Step 5: Weekly model retraining (Sunday)..."
    
    # Check if we have enough data changes to justify retraining
    ACCIDENT_CHANGES=$(PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t -c "
        SELECT COUNT(*) FROM accidents 
        WHERE created_date > (NOW() - INTERVAL '7 days');
    " | xargs)
    
    if [ "$ACCIDENT_CHANGES" -gt 0 ]; then
        log "Found $ACCIDENT_CHANGES new accidents - retraining model..."
        
        PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -c "
            -- Retrain model
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
        
        # Recalculate scores with new model
        log "Recalculating scores with updated model..."
        PGDATABASE="$TARGET_DATABASE" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -f recalculate_safe_driver_scores.sql >> "$LOG_FILE" 2>&1
    else
        log "No new accidents - skipping model retrain"
    fi
else
    log "Step 5: Skipping model retrain (not Sunday)"
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
    
    # Optional: Send notification (uncomment to enable)
    # echo "High-risk drivers detected: $HIGH_RISK_DRIVERS" | mail -s "Safe Driver Alert" admin@company.com
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