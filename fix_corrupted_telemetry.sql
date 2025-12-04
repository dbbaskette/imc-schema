-- =============================================================================
-- Temporary Fix for Corrupted Parquet File
-- =============================================================================
-- This creates a filtered view that skips the corrupted 2025-09-24 partition
-- =============================================================================

-- Create a working external table that excludes corrupted partitions
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
  -- Exclude the corrupted date=2025-09-24 partition
  'pxf://telemetry-data-v2/date=2024*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-01*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-02*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-03*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-04*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-05*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-06*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-07*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true',
  'pxf://telemetry-data-v2/date=2025-08*/telemetry-*.parquet?PROFILE=hdfs:parquet&SERVER=hdfs-server&IGNORE_MISSING_PATH=true'
  -- Skipping date=2025-09-24 due to corrupted file
)
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- Verify the working table
SELECT
    'Working table created - row count:' as status,
    COUNT(*) as rows
FROM vehicle_telemetry_data_v2_working;

-- Show available date partitions
SELECT
    'Available data sample:' as info,
    MIN(event_time::timestamp) as earliest_event,
    MAX(event_time::timestamp) as latest_event,
    COUNT(DISTINCT driver_id) as unique_drivers
FROM vehicle_telemetry_data_v2_working;
