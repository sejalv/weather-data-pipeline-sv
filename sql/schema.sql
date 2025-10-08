-- Energy Weather Data Pipeline - Database Schema
-- PostgreSQL 15+ with PostGIS 3.3+

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================================
-- RAW LAYER (Bronze) - Ingested data as-is from sources
-- ============================================================================

-- Postal code boundaries (from yetzt/postleitzahlen)
CREATE TABLE IF NOT EXISTS postal_codes (   -- plz
    id SERIAL PRIMARY KEY,
    postal_code VARCHAR(5) UNIQUE NOT NULL,
    city VARCHAR(255),
    state VARCHAR(255),
    geometry GEOMETRY(MULTIPOLYGON, 4326),  -- Changed from POLYGON to MULTIPOLYGON to match script
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_postal_codes_geometry ON postal_codes USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_postal_codes_postal_code ON postal_codes(postal_code);


-- DWD weather station metadata
CREATE TABLE IF NOT EXISTS weather_stations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(50) UNIQUE NOT NULL,
    station_name VARCHAR(255),
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    altitude INTEGER,
    source VARCHAR(100),
    location GEOMETRY(POINT, 4326),
    first_record_date TIMESTAMP,
    last_record_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_weather_stations_location ON weather_stations USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_weather_stations_station_id ON weather_stations(station_id);


-- Raw weather observations from BrightSky API
CREATE TABLE IF NOT EXISTS raw_weather_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL REFERENCES weather_stations(station_id),
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5, 2),
    humidity INTEGER,
    pressure DECIMAL(7, 2),
    wind_speed DECIMAL(5, 2),
    wind_direction INTEGER,
    precipitation DECIMAL(5, 2),
    cloud_cover INTEGER,
    visibility DECIMAL(8, 2),
    sunshine DECIMAL(5, 2),
    dew_point DECIMAL(5, 2),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    source VARCHAR(50),
    validated BOOLEAN DEFAULT FALSE,
    source_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(station_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_weather_observations_station_timestamp ON raw_weather_observations(station_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_observations_timestamp ON raw_weather_observations(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_observations_source_type ON raw_weather_observations(source_type);
CREATE INDEX IF NOT EXISTS idx_weather_observations_validated ON raw_weather_observations(validated);


-- Raw weather forecasts from BrightSky API
CREATE TABLE IF NOT EXISTS raw_weather_forecasts (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL REFERENCES weather_stations(station_id),
    forecast_timestamp TIMESTAMP NOT NULL,  -- when forecast was made
    target_timestamp TIMESTAMP NOT NULL,    -- forecast valid time
    temperature DECIMAL(5, 2),
    humidity INTEGER,
    pressure DECIMAL(7, 2),
    wind_speed DECIMAL(5, 2),
    wind_direction INTEGER,
    precipitation DECIMAL(5, 2),
    cloud_cover INTEGER,
    visibility DECIMAL(8, 2),
    sunshine DECIMAL(5, 2),
    dew_point DECIMAL(5, 2),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    source VARCHAR(50),
    validated BOOLEAN DEFAULT FALSE,
    source_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(station_id, forecast_timestamp, target_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_weather_forecasts_station_forecast ON raw_weather_forecasts(station_id, forecast_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_target_timestamp ON raw_weather_forecasts(target_timestamp DESC);


-- ============================================================================
-- STAGING LAYER (Silver) - Cleaned and validated data
-- ============================================================================

-- Quality-checked observations
CREATE TABLE IF NOT EXISTS stg_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL REFERENCES weather_stations(station_id),
    timestamp TIMESTAMP NOT NULL,
    
    -- Cleaned weather parameters
    temperature DECIMAL(5, 2),
    humidity INTEGER,
    pressure DECIMAL(7, 2),
    wind_speed DECIMAL(5, 2),
    wind_direction INTEGER,
    precipitation DECIMAL(5, 2),
    cloud_cover INTEGER,
    visibility DECIMAL(8, 2),
    sunshine DECIMAL(5, 2),
    dew_point DECIMAL(5, 2),
    
    -- Quality flags
    has_missing_values BOOLEAN DEFAULT FALSE,
    has_outliers BOOLEAN DEFAULT FALSE,
    interpolated_fields TEXT[],  -- Array of field names that were interpolated
    data_quality_score DECIMAL(3, 2) DEFAULT 1.0 CHECK (data_quality_score BETWEEN 0 AND 1),
    quality_notes TEXT,
        
    -- -- Data quality tracking
    -- is_interpolated BOOLEAN DEFAULT FALSE,
    -- is_outlier_detected BOOLEAN DEFAULT FALSE,
    -- contributing_stations TEXT[],  -- Array of station IDs used
    -- missing_data_flags JSONB,      -- Flexible metadata for missing fields
    -- created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_observation_id BIGINT REFERENCES raw_weather_observations(id),
    
    CONSTRAINT unique_stg_observation UNIQUE (station_id, timestamp),
    CONSTRAINT valid_quality_score CHECK (data_quality_score >= 0 AND data_quality_score <= 1)
);

CREATE INDEX IF NOT EXISTS idx_cleaned_observations_timestamp ON stg_observations(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_cleaned_observations_quality ON stg_observations(data_quality_score DESC);


-- Quality-checked forecasts
CREATE TABLE IF NOT EXISTS stg_forecasts (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL REFERENCES weather_stations(station_id),
    target_timestamp TIMESTAMP NOT NULL,
    forecast_timestamp TIMESTAMP NOT NULL,
    
    -- Cleaned weather parameters
    temperature DECIMAL(5, 2),
    humidity INTEGER,
    pressure DECIMAL(7, 2),
    wind_speed DECIMAL(5, 2),
    wind_direction INTEGER,
    precipitation DECIMAL(5, 2),
    cloud_cover INTEGER,
    visibility DECIMAL(8, 2),
    sunshine DECIMAL(5, 2),
    dew_point DECIMAL(5, 2),
    
    -- Quality flags (same as observations)
    has_missing_values BOOLEAN DEFAULT FALSE,
    has_outliers BOOLEAN DEFAULT FALSE,
    interpolated_fields TEXT[],
    data_quality_score DECIMAL(3, 2) CHECK (data_quality_score BETWEEN 0 AND 1) DEFAULT 1.0,
    quality_notes TEXT,
    
    -- -- Data quality and accuracy tracking
    -- forecast_accuracy_score DECIMAL(3, 2),  -- Calculated by comparing to actual observations
    -- is_interpolated BOOLEAN DEFAULT FALSE,
    -- is_outlier_detected BOOLEAN DEFAULT FALSE,
    -- contributing_stations TEXT[],
    -- created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_forecast_id BIGINT REFERENCES raw_weather_forecasts(id),
    
    CONSTRAINT unique_stg_forecast UNIQUE (station_id, forecast_timestamp, target_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_cleaned_forecasts_target_timestamp ON stg_forecasts(target_timestamp DESC);

-- ============================================================================
-- ANALYTICS LAYER (Gold) - ML-ready aggregated data
-- ============================================================================

-- Weather data aggregated by postal code and hour
CREATE TABLE IF NOT EXISTS analytics_weather_by_postal_code (
    id BIGSERIAL PRIMARY KEY,
    postal_code VARCHAR(5) NOT NULL REFERENCES postal_codes(postal_code),
    timestamp TIMESTAMP NOT NULL,
    data_type VARCHAR(20) NOT NULL,  -- 'observation' or 'forecast'
    
    -- Aggregated weather parameters (avg of nearby stations)
    temperature_avg NUMERIC(5,2),
    temperature_min NUMERIC(5,2),
    temperature_max NUMERIC(5,2),
    precipitation_sum NUMERIC(6,2),
    wind_speed_avg NUMERIC(5,2),
    wind_speed_max NUMERIC(5,2),
    wind_direction_avg INTEGER,
    cloud_cover_avg INTEGER,
    pressure_msl_avg NUMERIC(6,2),
    sunshine_sum INTEGER,
    visibility_avg INTEGER,
    dew_point_avg NUMERIC(5,2),
    relative_humidity_avg INTEGER,
    
    -- Quality metrics
    num_stations INTEGER NOT NULL,
    avg_quality_score NUMERIC(3,2),
    max_distance_km NUMERIC(8,2),  -- Distance to farthest station used
    
    -- For forecasts only
    forecast_timestamp TIMESTAMP,  -- NULL for observations
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_postal_weather UNIQUE (postal_code, timestamp, data_type, forecast_timestamp),
    CONSTRAINT valid_data_type CHECK (data_type IN ('observation', 'forecast')),
    CONSTRAINT forecast_has_forecast_timestamp CHECK (
        (data_type = 'forecast' AND forecast_timestamp IS NOT NULL) OR
        (data_type = 'observation' AND forecast_timestamp IS NULL)
    )
);

-- Forecast accuracy tracking (for ML model improvement)
CREATE TABLE IF NOT EXISTS analytics_forecast_accuracy (
    id BIGSERIAL PRIMARY KEY,
    postal_code VARCHAR(5) NOT NULL REFERENCES postal_codes(postal_code),
    forecast_issue_time TIMESTAMP NOT NULL,
    forecast_target_time TIMESTAMP NOT NULL,
    observed_time TIMESTAMP NOT NULL,
    
    -- Forecast vs observed differences
    temperature_error NUMERIC(5,2),
    precipitation_error NUMERIC(6,2),
    wind_speed_error NUMERIC(5,2),
    
    -- Aggregate metrics
    mae NUMERIC(6,3),  -- Mean Absolute Error
    rmse NUMERIC(6,3),  -- Root Mean Square Error
    
    -- Metadata
    lead_time_hours INTEGER,  -- Hours between issue and target
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_accuracy_record UNIQUE (postal_code, forecast_issue_time, forecast_target_time)
);

-- ============================================================================
-- INDEXES for Performance
-- ============================================================================

-- Spatial indexes for PostGIS queries
CREATE INDEX IF NOT EXISTS idx_postal_codes_geometry 
    ON postal_codes USING GIST (geometry);

CREATE INDEX IF NOT EXISTS idx_weather_stations_location 
    ON weather_stations USING GIST (location);

-- BRIN indexes for time-series (efficient for chronological data)
CREATE INDEX IF NOT EXISTS idx_raw_weather_observations_timestamp 
    ON raw_weather_observations USING BRIN (timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_weather_forecasts_forecast_timestamp 
    ON raw_weather_forecasts USING BRIN (forecast_timestamp);

CREATE INDEX IF NOT EXISTS idx_stg_observations_timestamp 
    ON stg_observations USING BRIN (timestamp);

CREATE INDEX IF NOT EXISTS idx_stg_forecasts_forecast_timestamp 
    ON stg_forecasts USING BRIN (forecast_timestamp);

CREATE INDEX IF NOT EXISTS idx_analytics_weather_timestamp 
    ON analytics_weather_by_postal_code USING BRIN (timestamp);

-- B-tree indexes for lookups and joins
CREATE INDEX IF NOT EXISTS idx_raw_observations_station_timestamp 
    ON raw_weather_observations (station_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_raw_weather_forecasts_station_times 
    ON raw_weather_forecasts (station_id, forecast_timestamp DESC, target_timestamp);

CREATE INDEX IF NOT EXISTS idx_stg_observations_station_timestamp 
    ON stg_observations (station_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_analytics_weather_postal_timestamp 
    ON analytics_weather_by_postal_code (postal_code, timestamp DESC, data_type);

-- ============================================================================
-- VIEWS
-- ============================================================================

-- Latest observations per postal code
CREATE OR REPLACE VIEW v_latest_observations AS
SELECT 
    postal_code,
    timestamp,
    temperature_avg,
    precipitation_sum,
    wind_speed_avg,
    cloud_cover_avg,
    num_stations,
    avg_quality_score
FROM analytics_weather_by_postal_code
WHERE data_type = 'observation'
    AND timestamp = (
        SELECT MAX(timestamp) 
        FROM analytics_weather_by_postal_code 
        WHERE data_type = 'observation'
    );

-- Latest forecasts per postal code
CREATE OR REPLACE VIEW v_latest_forecasts AS
WITH latest_forecast_issue AS (
    SELECT MAX(forecast_timestamp) as max_forecast_timestamp
    FROM analytics_weather_by_postal_code
    WHERE data_type = 'forecast'
)
SELECT 
    w.postal_code,
    w.forecast_timestamp,
    w.timestamp as target_time,
    w.temperature_avg,
    w.precipitation_sum,
    w.wind_speed_avg,
    w.cloud_cover_avg,
    w.num_stations
FROM analytics_weather_by_postal_code w
CROSS JOIN latest_forecast_issue l
WHERE w.data_type = 'forecast'
    AND w.forecast_timestamp = l.max_forecast_timestamp
ORDER BY w.postal_code, w.timestamp;

-- ============================================================================
-- FUNCTIONS for Common Operations
-- ============================================================================

-- Function to find nearest weather stations to a postal code
CREATE OR REPLACE FUNCTION get_nearest_stations(
    p_postal_code VARCHAR(5),
    p_limit INTEGER DEFAULT 3,
    p_max_distance_km NUMERIC DEFAULT 50
)
RETURNS TABLE (
    station_id VARCHAR(20),
    station_name VARCHAR(200),
    distance_km NUMERIC(8,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.station_id,
        s.station_name,
        ROUND(
            ST_Distance(
                ST_Centroid(p.geometry)::geography,
                s.location
            ) / 1000, 
            2
        ) AS distance_km
    FROM weather_stations s
    CROSS JOIN postal_codes p
    WHERE p.postal_code = p_postal_code
    ORDER BY ST_Centroid(p.geometry)::geography <-> s.location
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to update forecast accuracy after observations arrive
CREATE OR REPLACE FUNCTION calculate_forecast_accuracy(
    p_postal_code VARCHAR(5),
    p_observation_time TIMESTAMP
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO analytics_forecast_accuracy (
        postal_code,
        forecast_issue_time,
        forecast_target_time,
        observed_time,
        temperature_error,
        precipitation_error,
        wind_speed_error,
        mae,
        lead_time_hours
    )
    SELECT 
        f.postal_code,
        f.forecast_timestamp,
        f.timestamp,
        o.timestamp,
        ABS(f.temperature_avg - o.temperature_avg),
        ABS(f.precipitation_sum - o.precipitation_sum),
        ABS(f.wind_speed_avg - o.wind_speed_avg),
        (
            ABS(f.temperature_avg - o.temperature_avg) +
            ABS(COALESCE(f.precipitation_sum, 0) - COALESCE(o.precipitation_sum, 0)) +
            ABS(f.wind_speed_avg - o.wind_speed_avg)
        ) / 3.0,
        EXTRACT(EPOCH FROM (f.timestamp - f.forecast_timestamp)) / 3600
    FROM analytics_weather_by_postal_code f
    JOIN analytics_weather_by_postal_code o 
        ON f.postal_code = o.postal_code
        AND f.timestamp = o.timestamp
    WHERE f.postal_code = p_postal_code
        AND o.timestamp = p_observation_time
        AND f.data_type = 'forecast'
        AND o.data_type = 'observation'
        AND f.timestamp <= o.timestamp + INTERVAL '1 hour'
    ON CONFLICT (postal_code, forecast_issue_time, forecast_target_time)
    DO UPDATE SET
        observed_time = EXCLUDED.observed_time,
        temperature_error = EXCLUDED.temperature_error,
        precipitation_error = EXCLUDED.precipitation_error,
        wind_speed_error = EXCLUDED.wind_speed_error,
        mae = EXCLUDED.mae,
        calculated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS for Documentation
-- ============================================================================

COMMENT ON TABLE postal_codes IS 'German postal code boundaries from yetzt/postleitzahlen';
COMMENT ON TABLE weather_stations IS 'DWD weather station metadata';
COMMENT ON TABLE raw_weather_observations IS 'Hourly weather observations from BrightSky API';
COMMENT ON TABLE raw_weather_forecasts IS 'Hourly weather forecasts from BrightSky API';
COMMENT ON TABLE stg_observations IS 'Quality-checked and cleaned observations';
COMMENT ON TABLE stg_forecasts IS 'Quality-checked and cleaned forecasts';
COMMENT ON TABLE analytics_weather_by_postal_code IS 'ML-ready weather data aggregated to postal code level';
COMMENT ON TABLE analytics_forecast_accuracy IS 'Tracking forecast accuracy for model improvement';

COMMENT ON COLUMN analytics_weather_by_postal_code.avg_quality_score IS 'Average quality score from source stations (0.0-1.0)';
COMMENT ON COLUMN analytics_weather_by_postal_code.max_distance_km IS 'Distance to farthest station used in aggregation';
COMMENT ON COLUMN analytics_forecast_accuracy.mae IS 'Mean Absolute Error across temperature, precipitation, and wind speed';
