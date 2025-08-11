-- =============================================================================
-- Vehicle Telemetry Data External Table (PXF -> HDFS)
-- =============================================================================
-- This external table provides SQL access to vehicle telemetry data stored
-- in HDFS by the crash detection application. The data is written in Parquet
-- format with partitioning by policy_id, year, month, and date.
--
-- Data Source: imc-crash-detection app writes ALL telemetry to HDFS
-- Format: Parquet with Snappy compression
-- Partitioning: policy_id=XXX/year=YYYY/month=MM/date=YYYY-MM-DD/
-- =============================================================================

-- Create external table for vehicle telemetry data
CREATE EXTERNAL TABLE vehicle_telemetry_data (
    -- Core telemetry fields
    policy_id VARCHAR(50),
    vin VARCHAR(17),
    timestamp TIMESTAMP,
    speed_mph DOUBLE PRECISION,
    current_street VARCHAR(200),
    g_force DOUBLE PRECISION,
    
    -- GPS sensor data
    gps_latitude DOUBLE PRECISION,
    gps_longitude DOUBLE PRECISION,
    gps_altitude DOUBLE PRECISION,
    gps_speed_ms DOUBLE PRECISION,
    gps_bearing DOUBLE PRECISION,
    gps_accuracy DOUBLE PRECISION,
    gps_satellite_count INTEGER,
    gps_fix_time TIMESTAMP,
    
    -- Accelerometer data (3-axis acceleration in m/s²)
    accel_x DOUBLE PRECISION,
    accel_y DOUBLE PRECISION,
    accel_z DOUBLE PRECISION,
    
    -- Gyroscope data (angular velocity in rad/s)
    gyro_x DOUBLE PRECISION,
    gyro_y DOUBLE PRECISION,
    gyro_z DOUBLE PRECISION,
    
    -- Magnetometer data (magnetic field in µT)
    mag_x DOUBLE PRECISION,
    mag_y DOUBLE PRECISION,
    mag_z DOUBLE PRECISION,
    mag_heading DOUBLE PRECISION,
    
    -- Environmental sensors
    barometric_pressure DOUBLE PRECISION,
    
    -- Device health information
    device_battery_level DOUBLE PRECISION,
    device_signal_strength INTEGER,
    device_orientation VARCHAR(20),
    device_screen_on BOOLEAN,
    device_charging BOOLEAN,
    
    -- Processing metadata
    processed_timestamp TIMESTAMP,
    
    -- Partitioning columns (for directory structure)
    year VARCHAR(4),
    month VARCHAR(2),
    date VARCHAR(10),
    hour VARCHAR(2)
)
LOCATION ('pxf://__TELEMETRY_PATH__?PROFILE=__PXF_PROFILE__&SERVER=__PXF_SERVER__')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- Create indexes for common query patterns
CREATE INDEX idx_telemetry_policy_date ON vehicle_telemetry_data (policy_id, date);
CREATE INDEX idx_telemetry_vin_timestamp ON vehicle_telemetry_data (vin, timestamp);
CREATE INDEX idx_telemetry_gforce ON vehicle_telemetry_data (g_force) WHERE g_force > 2.0;

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