-- =============================================================================
-- Telemetry Analytics Views
-- =============================================================================
-- This file contains all analytical views for telemetry and crash data
-- =============================================================================

-- Create a view for easier querying with calculated fields
CREATE VIEW v_vehicle_telemetry_enriched AS
SELECT 
    vt.*,
    -- Calculate derived metrics
    SQRT(accel_x*accel_x + accel_y*accel_y + accel_z*accel_z) / 9.81 AS calculated_g_force,
    SQRT(accel_x*accel_x + accel_y*accel_y) / 9.81 AS lateral_g_force,
    ABS((accel_z - 9.81) / 9.81) AS vertical_g_force,
    SQRT(gyro_x*gyro_x + gyro_y*gyro_y + gyro_z*gyro_z) AS gyro_magnitude,
    
    -- Add vehicle and policy information via joins
    v.make,
    v.model,
    v.year AS vehicle_year,
    v.color,
    p.policy_number,
    p.status AS policy_status,
    c.first_name,
    c.last_name,
    c.email
FROM vehicle_telemetry_data vt
LEFT JOIN vehicles v ON vt.vin = v.vin
LEFT JOIN policies p ON vt.policy_id::INTEGER = p.policy_id
LEFT JOIN customers c ON p.customer_id = c.customer_id;

-- Create a view for high G-force events (potential crashes)
CREATE VIEW v_high_gforce_events AS
SELECT 
    policy_id,
    vin,
    timestamp,
    current_street,
    speed_mph,
    g_force,
    gps_latitude,
    gps_longitude,
    accel_x,
    accel_y,
    accel_z,
    SQRT(accel_x*accel_x + accel_y*accel_y + accel_z*accel_z) / 9.81 AS calculated_g_force,
    CASE 
        WHEN g_force >= 8.0 THEN 'CRITICAL'
        WHEN g_force >= 6.0 THEN 'SEVERE'
        WHEN g_force >= 4.0 THEN 'HIGH'
        WHEN g_force >= 2.0 THEN 'MODERATE'
        ELSE 'NORMAL'
    END AS severity_level
FROM vehicle_telemetry_data
WHERE g_force >= 2.0
ORDER BY g_force DESC, timestamp DESC;

-- Create a view for vehicle behavior analysis
CREATE VIEW v_vehicle_behavior_summary AS
SELECT 
    policy_id,
    vin,
    date,
    COUNT(*) AS record_count,
    AVG(speed_mph) AS avg_speed,
    MAX(speed_mph) AS max_speed,
    AVG(g_force) AS avg_g_force,
    MAX(g_force) AS max_g_force,
    COUNT(*) FILTER (WHERE g_force > 2.0) AS high_gforce_events,
    COUNT(*) FILTER (WHERE speed_mph > 80) AS speeding_events,
    AVG(device_battery_level) AS avg_battery_level,
    AVG(gps_accuracy) AS avg_gps_accuracy
FROM vehicle_telemetry_data
GROUP BY policy_id, vin, date
ORDER BY date DESC, policy_id;

-- Create a comprehensive view for crash analysis
CREATE VIEW v_crash_reports_enriched AS
SELECT 
    cr.*,
    -- Add vehicle and policy information
    v.make,
    v.model,
    v.year AS vehicle_year,
    v.color,
    p.policy_number,
    p.status AS policy_status,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address,
    c.city,
    c.state,
    c.zip_code,
    
    -- Calculate additional metrics
    CASE 
        WHEN emergency_recommended AND risk_score >= 90 THEN 'IMMEDIATE_RESPONSE'
        WHEN emergency_recommended AND risk_score >= 70 THEN 'URGENT_RESPONSE'
        WHEN risk_score >= 50 THEN 'STANDARD_RESPONSE'
        ELSE 'LOW_PRIORITY'
    END AS response_priority,
    
    -- Time-based analysis
    EXTRACT(HOUR FROM crash_timestamp) AS crash_hour,
    EXTRACT(DOW FROM crash_timestamp) AS crash_day_of_week,
    CASE 
        WHEN EXTRACT(HOUR FROM crash_timestamp) BETWEEN 6 AND 9 THEN 'MORNING_RUSH'
        WHEN EXTRACT(HOUR FROM crash_timestamp) BETWEEN 17 AND 19 THEN 'EVENING_RUSH'
        WHEN EXTRACT(HOUR FROM crash_timestamp) BETWEEN 22 AND 5 THEN 'NIGHT'
        ELSE 'DAYTIME'
    END AS time_category
    
FROM crash_reports_data cr
LEFT JOIN vehicles v ON cr.vin = v.vin
LEFT JOIN policies p ON cr.policy_id::INTEGER = p.policy_id
LEFT JOIN customers c ON p.customer_id = c.customer_id;

-- Create a view for emergency response prioritization
CREATE VIEW v_emergency_response_queue AS
SELECT 
    report_id,
    policy_id,
    vin,
    crash_timestamp,
    crash_type,
    severity_level,
    risk_score,
    current_street,
    crash_latitude,
    crash_longitude,
    first_name || ' ' || last_name AS customer_name,
    phone,
    make || ' ' || model || ' (' || vehicle_year || ')' AS vehicle_description,
    CASE 
        WHEN emergency_recommended AND risk_score >= 90 THEN 1
        WHEN emergency_recommended AND risk_score >= 70 THEN 2
        WHEN risk_score >= 50 THEN 3
        ELSE 4
    END AS response_priority,
    EXTRACT(EPOCH FROM (NOW() - crash_timestamp))/60 AS minutes_since_crash
FROM v_crash_reports_enriched
WHERE crash_timestamp >= (NOW() - INTERVAL '24 hours')
  AND emergency_recommended = true
ORDER BY response_priority, crash_timestamp DESC;

-- Create a view for crash pattern analysis
CREATE VIEW v_crash_patterns AS
SELECT 
    crash_type,
    severity_level,
    COUNT(*) AS crash_count,
    AVG(risk_score) AS avg_risk_score,
    AVG(total_g_force) AS avg_g_force,
    AVG(speed_at_impact) AS avg_impact_speed,
    COUNT(*) FILTER (WHERE emergency_recommended) AS emergency_cases,
    COUNT(*) FILTER (WHERE rollover_detected) AS rollover_cases,
    COUNT(*) FILTER (WHERE spinning_detected) AS spinning_cases,
    
    -- Time patterns
    AVG(EXTRACT(HOUR FROM crash_timestamp)) AS avg_crash_hour,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM crash_timestamp) IN (0,6)) AS weekend_crashes,
    
    -- Location clustering (simplified)
    AVG(crash_latitude) AS avg_latitude,
    AVG(crash_longitude) AS avg_longitude,
    
    -- Most recent occurrences
    MAX(crash_timestamp) AS latest_crash,
    MIN(crash_timestamp) AS earliest_crash
    
FROM crash_reports_data
GROUP BY crash_type, severity_level
ORDER BY crash_count DESC, avg_risk_score DESC;

-- Create a view for geographic crash hotspots
CREATE VIEW v_crash_hotspots AS
SELECT 
    ROUND(crash_latitude::NUMERIC, 3) AS lat_zone,
    ROUND(crash_longitude::NUMERIC, 3) AS lng_zone,
    COUNT(*) AS crash_count,
    AVG(risk_score) AS avg_risk_score,
    COUNT(*) FILTER (WHERE severity_level = 'CRITICAL') AS critical_crashes,
    COUNT(*) FILTER (WHERE emergency_recommended) AS emergency_crashes,
    string_agg(DISTINCT current_street, ', ') AS streets,
    MAX(crash_timestamp) AS latest_crash
FROM crash_reports_data
GROUP BY ROUND(crash_latitude::NUMERIC, 3), ROUND(crash_longitude::NUMERIC, 3)
HAVING COUNT(*) >= 2  -- Only show areas with multiple crashes
ORDER BY crash_count DESC, avg_risk_score DESC;