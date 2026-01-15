-- Drop and recreate vehicle_events table for JDBC sink compatibility
-- This script changes event_timestamp from TIMESTAMP WITH TIME ZONE to BIGINT
-- to accept numeric timestamps directly from the telemetry processor

-- Drop the existing table if it exists (CASCADE to drop dependent views)
DROP TABLE IF EXISTS vehicle_events CASCADE;

-- Recreate the table with BIGINT for event_timestamp
CREATE TABLE vehicle_events (
    policy_id BIGINT,
    vehicle_id BIGINT,
    vin VARCHAR(255),
    event_time TIMESTAMP,
    speed_mph REAL,
    speed_limit_mph REAL,
    current_street VARCHAR(255),
    g_force REAL,
    driver_id INTEGER,
    
    -- GPS sensor data (flattened from sensors.gps)
    gps_latitude DOUBLE PRECISION,
    gps_longitude DOUBLE PRECISION,
    gps_altitude DOUBLE PRECISION,
    gps_speed REAL,
    gps_bearing REAL,
    gps_accuracy REAL,
    gps_satellite_count INTEGER,
    gps_fix_time INTEGER,
    
    -- Accelerometer data (flattened from sensors.accelerometer)
    accelerometer_x REAL,
    accelerometer_y REAL,
    accelerometer_z REAL,
    
    -- Gyroscope data (flattened from sensors.gyroscope)
    gyroscope_x REAL,
    gyroscope_y REAL,
    gyroscope_z REAL,
    
    -- Magnetometer data (flattened from sensors.magnetometer)
    magnetometer_x REAL,
    magnetometer_y REAL,
    magnetometer_z REAL,
    magnetometer_heading REAL,
    
    -- Environmental sensors
    barometric_pressure REAL,
    
    -- Device info (flattened from sensors.device)
    device_battery_level REAL,
    device_signal_strength INTEGER,
    device_orientation VARCHAR(255),
    device_screen_on BOOLEAN,
    device_charging BOOLEAN
)
WITH (
    APPENDONLY=true,
    OIDS=FALSE
)
DISTRIBUTED BY (vehicle_id);

-- Add indexes for performance
DROP INDEX IF EXISTS idx_vehicle_events_event_time;
DROP INDEX IF EXISTS idx_vehicle_events_policy_id;  
DROP INDEX IF EXISTS idx_vehicle_events_vehicle_id;
DROP INDEX IF EXISTS idx_vehicle_events_driver_id;

CREATE INDEX idx_vehicle_events_event_time ON vehicle_events (event_time);
CREATE INDEX idx_vehicle_events_policy_id ON vehicle_events (policy_id);
CREATE INDEX idx_vehicle_events_vehicle_id ON vehicle_events (vehicle_id);
CREATE INDEX idx_vehicle_events_driver_id ON vehicle_events (driver_id);


-- Create a view for easy querying and analysis
CREATE VIEW vehicle_events_view AS
SELECT 
    policy_id,
    vehicle_id,
    vin,
    event_time,
    speed_mph,
    speed_limit_mph,
    current_street,
    g_force,
    driver_id,
    
    -- GPS data
    gps_latitude,
    gps_longitude,
    gps_altitude,
         gps_speed,
    gps_bearing,
    gps_accuracy,
    gps_satellite_count,
    gps_fix_time,
    
    -- Motion sensors
    accelerometer_x,
    accelerometer_y,
    accelerometer_z,
         gyroscope_x,
     gyroscope_y,
     gyroscope_z,
    magnetometer_x,
    magnetometer_y,
    magnetometer_z,
    magnetometer_heading,
    
         -- Environmental
     barometric_pressure,
    
    -- Device info
    device_battery_level,
    device_signal_strength,
    device_orientation,
    device_screen_on,
    device_charging,
    
    -- Calculated fields
    CASE WHEN speed_mph > speed_limit_mph THEN true ELSE false END AS is_speeding,
    (speed_mph - speed_limit_mph) AS speed_over_limit
FROM vehicle_events;

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON vehicle_events TO your_user;
-- GRANT SELECT ON vehicle_events_view TO your_user;
