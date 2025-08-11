-- =============================================================================
-- Sample Telemetry Data Queries
-- =============================================================================
-- This file contains sample SQL queries to validate and demonstrate
-- the capabilities of the HDFS telemetry data external tables.
-- 
-- Prerequisites:
-- 1. PXF service must be running and configured
-- 2. HDFS must contain telemetry data in the expected format
-- 3. External tables must be created successfully
-- =============================================================================

-- =============================================================================
-- BASIC VALIDATION QUERIES
-- =============================================================================

-- 1. Verify external table connectivity and basic data access
SELECT 'External Table Connectivity Test' AS test_name;
SELECT COUNT(*) AS total_telemetry_records FROM vehicle_telemetry_data LIMIT 1;

-- 2. Check data freshness and date range
SELECT 'Data Freshness Check' AS test_name;
SELECT 
    MIN(timestamp) AS earliest_record,
    MAX(timestamp) AS latest_record,
    COUNT(DISTINCT date) AS unique_dates,
    COUNT(DISTINCT policy_id) AS unique_policies
FROM vehicle_telemetry_data;

-- 3. Verify crash reports connectivity
SELECT 'Crash Reports Connectivity Test' AS test_name;
SELECT COUNT(*) AS total_crash_reports FROM crash_reports_data LIMIT 1;

-- =============================================================================
-- TELEMETRY DATA ANALYSIS QUERIES
-- =============================================================================

-- 4. Vehicle behavior summary by policy
SELECT 'Vehicle Behavior Analysis' AS analysis_type;
SELECT * FROM v_vehicle_behavior_summary 
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY max_g_force DESC
LIMIT 10;

-- 5. High G-force events detection
SELECT 'High G-Force Events' AS analysis_type;
SELECT 
    policy_id,
    vin,
    timestamp,
    current_street,
    g_force,
    speed_mph,
    severity_level
FROM v_high_gforce_events
WHERE timestamp >= CURRENT_DATE - INTERVAL '24 hours'
ORDER BY g_force DESC
LIMIT 20;

-- 6. Speeding behavior analysis
SELECT 'Speeding Analysis' AS analysis_type;
SELECT 
    policy_id,
    vin,
    AVG(speed_mph) AS avg_speed,
    MAX(speed_mph) AS max_speed,
    COUNT(*) FILTER (WHERE speed_mph > 80) AS speeding_violations,
    COUNT(*) FILTER (WHERE speed_mph > 90) AS severe_speeding,
    COUNT(*) AS total_records
FROM vehicle_telemetry_data
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY policy_id, vin
HAVING COUNT(*) FILTER (WHERE speed_mph > 80) > 0
ORDER BY speeding_violations DESC, max_speed DESC
LIMIT 15;

-- 7. Device health monitoring
SELECT 'Device Health Analysis' AS analysis_type;
SELECT 
    policy_id,
    vin,
    AVG(device_battery_level) AS avg_battery,
    MIN(device_battery_level) AS min_battery,
    AVG(device_signal_strength) AS avg_signal,
    COUNT(*) FILTER (WHERE device_battery_level < 0.2) AS low_battery_events,
    COUNT(*) FILTER (WHERE device_signal_strength < -80) AS poor_signal_events,
    MAX(timestamp) AS last_seen
FROM vehicle_telemetry_data
WHERE date >= CURRENT_DATE - INTERVAL '3 days'
GROUP BY policy_id, vin
ORDER BY low_battery_events DESC, poor_signal_events DESC
LIMIT 10;

-- =============================================================================
-- CRASH REPORTS ANALYSIS QUERIES
-- =============================================================================

-- 8. Recent crash reports summary
SELECT 'Recent Crashes Summary' AS analysis_type;
SELECT 
    crash_type,
    severity_level,
    COUNT(*) AS crash_count,
    AVG(risk_score) AS avg_risk_score,
    COUNT(*) FILTER (WHERE emergency_recommended) AS emergency_cases
FROM crash_reports_data
WHERE report_date >= (CURRENT_DATE - INTERVAL '30 days')::VARCHAR
GROUP BY crash_type, severity_level
ORDER BY crash_count DESC;

-- 9. Emergency response queue
SELECT 'Emergency Response Queue' AS analysis_type;
SELECT * FROM v_emergency_response_queue
LIMIT 10;

-- 10. Crash patterns analysis
SELECT 'Crash Patterns Analysis' AS analysis_type;
SELECT * FROM v_crash_patterns
LIMIT 10;

-- 11. Geographic crash hotspots
SELECT 'Crash Hotspots Analysis' AS analysis_type;
SELECT * FROM v_crash_hotspots
LIMIT 10;

-- =============================================================================
-- INTEGRATED ANALYSIS QUERIES
-- =============================================================================

-- 12. Customer risk profile with telemetry and crash history
SELECT 'Customer Risk Profiles' AS analysis_type;
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(DISTINCT vt.vin) AS vehicles_count,
    
    -- Telemetry-based risk indicators
    AVG(vt.g_force) AS avg_g_force,
    COUNT(*) FILTER (WHERE vt.g_force > 2.0) AS high_gforce_events,
    AVG(vt.speed_mph) AS avg_speed,
    COUNT(*) FILTER (WHERE vt.speed_mph > 80) AS speeding_events,
    
    -- Crash history
    COUNT(cr.report_id) AS crash_count,
    MAX(cr.risk_score) AS max_crash_risk_score,
    COUNT(*) FILTER (WHERE cr.emergency_recommended) AS emergency_crashes,
    
    -- Calculate composite risk score
    CASE 
        WHEN COUNT(cr.report_id) > 0 THEN 'HIGH_RISK'
        WHEN COUNT(*) FILTER (WHERE vt.g_force > 2.0) > 10 
             OR COUNT(*) FILTER (WHERE vt.speed_mph > 80) > 20 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_category
    
FROM customers c
JOIN policies p ON c.customer_id = p.customer_id
LEFT JOIN vehicle_telemetry_data vt ON p.policy_id::VARCHAR = vt.policy_id
    AND vt.date >= (CURRENT_DATE - INTERVAL '30 days')::VARCHAR
LEFT JOIN crash_reports_data cr ON p.policy_id::VARCHAR = cr.policy_id
    AND cr.report_date >= (CURRENT_DATE - INTERVAL '365 days')::VARCHAR
WHERE p.status = 'ACTIVE'
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
HAVING COUNT(DISTINCT vt.timestamp) > 0  -- Only customers with telemetry data
ORDER BY crash_count DESC, high_gforce_events DESC
LIMIT 20;

-- 13. Vehicle performance and safety correlation
SELECT 'Vehicle Safety Analysis' AS analysis_type;
SELECT 
    v.make,
    v.model,
    v.year,
    COUNT(DISTINCT vt.vin) AS vehicle_count,
    
    -- Safety metrics
    AVG(vt.g_force) AS avg_g_force,
    COUNT(cr.report_id) AS total_crashes,
    COUNT(cr.report_id)::FLOAT / COUNT(DISTINCT vt.vin) AS crashes_per_vehicle,
    
    -- Performance metrics
    AVG(vt.speed_mph) AS avg_speed,
    AVG(vt.device_battery_level) AS avg_device_health,
    AVG(vt.gps_accuracy) AS avg_gps_accuracy
    
FROM vehicles v
LEFT JOIN vehicle_telemetry_data vt ON v.vin = vt.vin
    AND vt.date >= (CURRENT_DATE - INTERVAL '90 days')::VARCHAR
LEFT JOIN crash_reports_data cr ON v.vin = cr.vin
    AND cr.report_date >= (CURRENT_DATE - INTERVAL '365 days')::VARCHAR
GROUP BY v.make, v.model, v.year
HAVING COUNT(DISTINCT vt.timestamp) > 100  -- Sufficient data for analysis
ORDER BY crashes_per_vehicle DESC, avg_g_force DESC
LIMIT 15;

-- =============================================================================
-- DATA QUALITY AND MONITORING QUERIES
-- =============================================================================

-- 14. Data completeness check
SELECT 'Data Quality Check' AS check_type;
SELECT 
    'vehicle_telemetry_data' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE policy_id IS NULL) AS missing_policy_id,
    COUNT(*) FILTER (WHERE vin IS NULL) AS missing_vin,
    COUNT(*) FILTER (WHERE gps_latitude IS NULL) AS missing_gps,
    COUNT(*) FILTER (WHERE device_battery_level IS NULL) AS missing_device_data,
    COUNT(DISTINCT date) AS unique_dates,
    COUNT(DISTINCT policy_id) AS unique_policies
FROM vehicle_telemetry_data
WHERE date >= (CURRENT_DATE - INTERVAL '7 days')::VARCHAR

UNION ALL

SELECT 
    'crash_reports_data' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE policy_id IS NULL) AS missing_policy_id,
    COUNT(*) FILTER (WHERE vin IS NULL) AS missing_vin,
    COUNT(*) FILTER (WHERE crash_latitude IS NULL) AS missing_location,
    COUNT(*) FILTER (WHERE risk_score IS NULL) AS missing_risk_score,
    COUNT(DISTINCT report_date) AS unique_dates,
    COUNT(DISTINCT policy_id) AS unique_policies
FROM crash_reports_data
WHERE report_date >= (CURRENT_DATE - INTERVAL '30 days')::VARCHAR;

-- 15. Performance monitoring query
SELECT 'Query Performance Test' AS test_type;
SELECT 
    COUNT(*) AS record_count,
    MIN(timestamp) AS time_range_start,
    MAX(timestamp) AS time_range_end,
    COUNT(DISTINCT policy_id) AS unique_policies,
    AVG(g_force) AS avg_g_force
FROM vehicle_telemetry_data
WHERE date = CURRENT_DATE::VARCHAR
  AND g_force > 1.0;