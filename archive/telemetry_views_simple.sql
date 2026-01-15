-- =============================================================================
-- Telemetry Analytics Views (Simple Version)
-- =============================================================================
-- This file contains basic analytical views for telemetry data only
-- =============================================================================

-- Create a view for easier querying with calculated fields
CREATE VIEW v_vehicle_telemetry_enriched AS
SELECT 
    vt.*,
    -- Calculate derived metrics
    SQRT(accelerometer_x*accelerometer_x + accelerometer_y*accelerometer_y + accelerometer_z*accelerometer_z) / 9.81 AS calculated_g_force,
    SQRT(accelerometer_x*accelerometer_x + accelerometer_y*accelerometer_y) / 9.81 AS lateral_g_force,
    ABS((accelerometer_z - 9.81) / 9.81) AS vertical_g_force,
    SQRT(gyroscope_pitch*gyroscope_pitch + gyroscope_roll*gyroscope_roll + gyroscope_yaw*gyroscope_yaw) AS gyro_magnitude,
    
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
    event_time,
    current_street,
    speed_mph,
    speed_limit_mph,
    g_force,
    gps_latitude,
    gps_longitude,
    accelerometer_x,
    accelerometer_y,
    accelerometer_z,
    SQRT(accelerometer_x*accelerometer_x + accelerometer_y*accelerometer_y + accelerometer_z*accelerometer_z) / 9.81 AS calculated_g_force,
    CASE 
        WHEN g_force >= 8.0 THEN 'CRITICAL'
        WHEN g_force >= 6.0 THEN 'SEVERE'
        WHEN g_force >= 4.0 THEN 'HIGH'
        WHEN g_force >= 2.0 THEN 'MODERATE'
        ELSE 'NORMAL'
    END AS severity_level
FROM vehicle_telemetry_data
WHERE g_force >= 2.0
ORDER BY g_force DESC, event_time DESC;

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
    COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph) AS speed_limit_violations,
    AVG(device_battery_level) AS avg_battery_level,
    AVG(gps_accuracy) AS avg_gps_accuracy
FROM vehicle_telemetry_data
GROUP BY policy_id, vin, date
ORDER BY date DESC, policy_id;