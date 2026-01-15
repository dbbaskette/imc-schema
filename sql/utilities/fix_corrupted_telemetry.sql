-- =============================================================================
-- Working Table for Telemetry Data
-- =============================================================================
-- Creates a working external table pointing to all telemetry partitions.
-- Run cleanup_telemetry_files.sh BEFORE this to remove corrupted files.
-- =============================================================================

DROP EXTERNAL TABLE IF EXISTS vehicle_telemetry_data_v2_working CASCADE;

CREATE EXTERNAL TABLE vehicle_telemetry_data_v2_working (
  -- Core telemetry fields
  policy_id              BIGINT,
  vehicle_id             BIGINT,
  vin                    VARCHAR(17),
  event_time             TEXT,
  speed_mph              DOUBLE PRECISION,
  speed_limit_mph        INTEGER,
  current_street         VARCHAR(200),
  g_force                DOUBLE PRECISION,
  driver_id              INTEGER,

  -- GPS sensor data (flattened)
  gps_latitude           DOUBLE PRECISION,
  gps_longitude          DOUBLE PRECISION,
  gps_altitude           DOUBLE PRECISION,
  gps_speed              DOUBLE PRECISION,
  gps_bearing            DOUBLE PRECISION,
  gps_accuracy           DOUBLE PRECISION,
  gps_satellite_count    INTEGER,
  gps_fix_time           INTEGER,

  -- Accelerometer data (flattened)
  accelerometer_x        DOUBLE PRECISION,
  accelerometer_y        DOUBLE PRECISION,
  accelerometer_z        DOUBLE PRECISION,

  -- Gyroscope data (flattened)
  gyroscope_x            DOUBLE PRECISION,
  gyroscope_y            DOUBLE PRECISION,
  gyroscope_z            DOUBLE PRECISION,

  -- Magnetometer data (flattened)
  magnetometer_x         DOUBLE PRECISION,
  magnetometer_y         DOUBLE PRECISION,
  magnetometer_z         DOUBLE PRECISION,
  magnetometer_heading   DOUBLE PRECISION,

  -- Environmental sensors
  barometric_pressure    DOUBLE PRECISION,

  -- Device info (flattened)
  device_battery_level   INTEGER,
  device_signal_strength INTEGER,
  device_orientation     VARCHAR(20),
  device_screen_on       BOOLEAN,
  device_charging        BOOLEAN
)
LOCATION (
  'pxf://telemetry-data-v2/date=*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true'
)
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- Verify the working table
SELECT
    'Working table created - row count:' as status,
    COUNT(*) as rows
FROM vehicle_telemetry_data_v2_working;

-- Show available date partitions (using TO_TIMESTAMP for epoch times stored as scientific notation)
SELECT
    'Available data sample:' as info,
    TO_TIMESTAMP(MIN(event_time::double precision)) as earliest_event,
    TO_TIMESTAMP(MAX(event_time::double precision)) as latest_event,
    COUNT(DISTINCT driver_id) as unique_drivers
FROM vehicle_telemetry_data_v2_working;
