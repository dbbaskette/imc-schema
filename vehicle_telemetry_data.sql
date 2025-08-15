-- =============================================================================
-- Vehicle Telemetry Data External Table (PXF -> HDFS)
-- =============================================================================
-- This external table provides SQL access to vehicle telemetry data stored
-- in HDFS by the imc-hdfs-sink application. The data is written in Parquet
-- format with date and driver-based partitioning.
--
-- Data Source: imc-hdfs-sink writes flattened telemetry to HDFS
-- Format: Parquet with Snappy compression
-- Partitioning: /YYYY-MM-DD/driver_id/telemetry-*.parquet
-- Path: /insurance-megacorp/telemetry-data-v2/
-- =============================================================================

-- Drop and recreate external table for vehicle telemetry data
DROP EXTERNAL TABLE IF EXISTS vehicle_telemetry_data CASCADE;
CREATE EXTERNAL TABLE vehicle_telemetry_data (
    -- Core telemetry fields
    policy_id BIGINT,
    vehicle_id BIGINT,
    vin VARCHAR(17),
    event_time TIMESTAMP,
    speed_mph DOUBLE PRECISION,
    speed_limit_mph DOUBLE PRECISION,
    current_street VARCHAR(200),
    g_force DOUBLE PRECISION,
    driver_id INTEGER,
    
    -- GPS sensor data (flattened)
    gps_latitude DOUBLE PRECISION,
    gps_longitude DOUBLE PRECISION,
    gps_altitude DOUBLE PRECISION,
    gps_speed DOUBLE PRECISION,
    gps_bearing DOUBLE PRECISION,
    gps_accuracy DOUBLE PRECISION,
    gps_satellite_count INTEGER,
    gps_fix_time INTEGER,
    
    -- Accelerometer data (flattened)
    accelerometer_x DOUBLE PRECISION,
    accelerometer_y DOUBLE PRECISION,
    accelerometer_z DOUBLE PRECISION,
    
    -- Gyroscope data (flattened)
    gyroscope_x DOUBLE PRECISION,
    gyroscope_y DOUBLE PRECISION,
    gyroscope_z DOUBLE PRECISION,
    
    -- Magnetometer data (flattened)
    magnetometer_x DOUBLE PRECISION,
    magnetometer_y DOUBLE PRECISION,
    magnetometer_z DOUBLE PRECISION,
    magnetometer_heading DOUBLE PRECISION,
    
    -- Environmental sensors
    barometric_pressure DOUBLE PRECISION,
    
    -- Device info (flattened)
    device_battery_level DOUBLE PRECISION,
    device_signal_strength INTEGER,
    device_orientation VARCHAR(20),
    device_screen_on BOOLEAN,
    device_charging BOOLEAN,
    
    -- Partitioning columns (for directory structure)
    date VARCHAR(10),
    driver_id_partition INTEGER
)
LOCATION ('pxf://__TELEMETRY_PATH__?PROFILE=__PXF_PROFILE__&SERVER=__PXF_SERVER__')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');