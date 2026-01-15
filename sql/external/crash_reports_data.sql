-- =============================================================================
-- Crash Reports Data External Table (PXF -> HDFS)
-- =============================================================================
-- This external table provides SQL access to processed crash reports stored
-- in HDFS by the crash detection application. Only crash events are written
-- to this location, with enhanced analysis and risk scoring.
--
-- Data Source: imc-crash-detection app writes crash reports to HDFS
-- Format: Parquet with Snappy compression
-- Partitioning: severity_level/report_date
-- =============================================================================

-- Create external table for crash reports data
CREATE EXTERNAL TABLE crash_reports_data (
    -- Core crash report identification
    report_id VARCHAR(100),
    policy_id VARCHAR(50),
    vin VARCHAR(17),
    crash_timestamp TIMESTAMP,
    crash_type VARCHAR(50),
    severity_level VARCHAR(20),
    
    -- Impact analysis details
    total_g_force DOUBLE PRECISION,
    lateral_g_force DOUBLE PRECISION,
    vertical_g_force DOUBLE PRECISION,
    speed_at_impact DOUBLE PRECISION,
    deceleration_rate DOUBLE PRECISION,
    
    -- Acceleration vector details
    impact_accel_x DOUBLE PRECISION,
    impact_accel_y DOUBLE PRECISION,
    impact_accel_z DOUBLE PRECISION,
    impact_magnitude DOUBLE PRECISION,
    
    -- Location information
    crash_latitude DOUBLE PRECISION,
    crash_longitude DOUBLE PRECISION,
    current_street VARCHAR(200),
    heading DOUBLE PRECISION,
    accuracy_meters DOUBLE PRECISION,
    
    -- Sensor analysis results
    accelerometer_raw_x DOUBLE PRECISION,
    accelerometer_raw_y DOUBLE PRECISION,
    accelerometer_raw_z DOUBLE PRECISION,
    accelerometer_total_magnitude DOUBLE PRECISION,
    accelerometer_lateral_magnitude DOUBLE PRECISION,
    accelerometer_vertical_magnitude DOUBLE PRECISION,
    
    gyroscope_x DOUBLE PRECISION,
    gyroscope_y DOUBLE PRECISION,
    gyroscope_z DOUBLE PRECISION,
    gyroscope_magnitude DOUBLE PRECISION,
    rollover_detected BOOLEAN,
    spinning_detected BOOLEAN,
    
    magnetometer_x DOUBLE PRECISION,
    magnetometer_y DOUBLE PRECISION,
    magnetometer_z DOUBLE PRECISION,
    magnetometer_heading_degrees DOUBLE PRECISION,
    
    -- Risk assessment
    emergency_recommended BOOLEAN,
    risk_score DOUBLE PRECISION,
    
    -- Processing metadata
    processed_timestamp TIMESTAMP,
    
    -- Partitioning columns
    report_date VARCHAR(10),
    report_hour VARCHAR(2)
)
LOCATION ('pxf://__CRASH_PATH__?PROFILE=__PXF_PROFILE__&SERVER=__PXF_SERVER__')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- Create indexes for crash report queries
CREATE INDEX idx_crash_reports_policy_date ON crash_reports_data (policy_id, report_date);
CREATE INDEX idx_crash_reports_vin_timestamp ON crash_reports_data (vin, crash_timestamp);
CREATE INDEX idx_crash_reports_severity ON crash_reports_data (severity_level);
CREATE INDEX idx_crash_reports_risk_score ON crash_reports_data (risk_score) WHERE risk_score >= 70.0;
CREATE INDEX idx_crash_reports_location ON crash_reports_data (crash_latitude, crash_longitude);

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