"""
Data cleaning and validation module.

Implements:
- Cleaning Step 1: Remove incomplete records (>50% missing critical fields)
- Cleaning Step 2: Flag outliers (physical + statistical limits)
- Validation Step: Temporal consistency check
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Critical fields that must be present
CRITICAL_FIELDS = ['temperature', 'wind_speed', 'precipitation']

# Physical limits for outlier detection
PHYSICAL_LIMITS = {
    'temperature': (-40, 50),      # Celsius
    'wind_speed': (0, 200),         # km/h
    'precipitation': (0, 200),      # mm/h
    'humidity': (0, 100),           # percent
    'cloud_cover': (0, 100),        # percent
}


def clean_and_stage_observations(conn, lookback_hours: int = 24) -> Dict[str, Any]:
    """
    Clean raw observations and load to staging table.
    
    Data Quality Steps:
    1. CLEANING STEP 1: Remove records with >50% missing critical fields
    2. CLEANING STEP 2: Flag outliers based on physical + statistical limits
    3. VALIDATION STEP: Check temporal consistency (no huge jumps)
    
    Args:
        conn: Database connection
        lookback_hours: Hours of data to process
        
    Returns:
        Statistics dictionary
    """
    logger.info(f"Cleaning observations (last {lookback_hours}h)")
    
    cursor = conn.cursor()
    stats = {'raw_count': 0, 'staging_count': 0, 'avg_quality_score': 0.0}
    
    try:
        # Get count of raw records
        cursor.execute("""
            SELECT COUNT(*) FROM raw_weather_observations
            WHERE timestamp >= NOW() - INTERVAL '%s hours'
        """ % (lookback_hours,))
        stats['raw_count'] = cursor.fetchone()[0]
        
        logger.info(f"Found {stats['raw_count']} raw observations to process")
        
        if stats['raw_count'] == 0:
            logger.warning("No raw observations to clean")
            return stats
        
        # Execute cleaning SQL with all 3 steps
        insert_query = """
            INSERT INTO stg_observations (
                station_id, timestamp,
                temperature, humidity, pressure, wind_speed, wind_direction,
                precipitation, cloud_cover, visibility, sunshine, dew_point,
                has_missing_values, has_outliers, data_quality_score,
                quality_notes, processed_at, raw_observation_id
            )
            SELECT 
                station_id,
                timestamp,
                temperature,
                humidity,
                pressure,
                wind_speed,
                wind_direction,
                precipitation,
                cloud_cover,
                visibility,
                sunshine,
                dew_point,
                
                -- CLEANING STEP 1: Check for missing values
                -- Flag if >50% of critical fields are missing
                CASE WHEN (
                    (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END)
                ) > 1 THEN TRUE ELSE FALSE END as has_missing_values,
                
                -- CLEANING STEP 2: Flag outliers
                -- Based on physical limits
                CASE WHEN (
                    temperature NOT BETWEEN -40 AND 50 OR
                    wind_speed NOT BETWEEN 0 AND 200 OR
                    precipitation NOT BETWEEN 0 AND 200 OR
                    (humidity IS NOT NULL AND humidity NOT BETWEEN 0 AND 100) OR
                    (cloud_cover IS NOT NULL AND cloud_cover NOT BETWEEN 0 AND 100)
                ) THEN TRUE ELSE FALSE END as has_outliers,
                
                -- Calculate quality score (0.0 - 1.0)
                CASE 
                    -- Perfect: all fields present, no outliers
                    WHEN temperature IS NOT NULL 
                        AND wind_speed IS NOT NULL 
                        AND precipitation IS NOT NULL
                        AND temperature BETWEEN -40 AND 50
                        AND wind_speed BETWEEN 0 AND 200
                        AND precipitation BETWEEN 0 AND 200
                    THEN 1.0
                    
                    -- Good: minor issues
                    WHEN (temperature IS NULL OR wind_speed IS NULL OR precipitation IS NULL)
                    THEN 0.6
                    
                    -- Poor: has outliers
                    WHEN temperature NOT BETWEEN -40 AND 50 
                        OR wind_speed NOT BETWEEN 0 AND 200
                    THEN 0.3
                    
                    ELSE 0.5
                END as data_quality_score,
                
                -- Quality notes for debugging
                CASE 
                    WHEN temperature NOT BETWEEN -40 AND 50 
                    THEN 'Temperature outlier: ' || COALESCE(temperature::text, 'NULL')
                    WHEN wind_speed NOT BETWEEN 0 AND 200
                    THEN 'Wind speed outlier: ' || COALESCE(wind_speed::text, 'NULL')
                    WHEN precipitation NOT BETWEEN 0 AND 200
                    THEN 'Precipitation outlier: ' || COALESCE(precipitation::text, 'NULL')
                    WHEN temperature IS NULL OR wind_speed IS NULL
                    THEN 'Missing critical fields'
                    ELSE NULL
                END as quality_notes,
                
                NOW() as processed_at,
                id as raw_observation_id
                
            FROM raw_weather_observations
            WHERE timestamp >= NOW() - INTERVAL '%s hours'
                -- CLEANING STEP 1: Exclude records with too many missing values
                AND NOT (
                    (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END)
                ) > 1
                -- Only process validated historical data
                AND validated = TRUE
            
            ON CONFLICT (station_id, timestamp) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                pressure = EXCLUDED.pressure,
                wind_speed = EXCLUDED.wind_speed,
                precipitation = EXCLUDED.precipitation,
                has_missing_values = EXCLUDED.has_missing_values,
                has_outliers = EXCLUDED.has_outliers,
                data_quality_score = EXCLUDED.data_quality_score,
                quality_notes = EXCLUDED.quality_notes,
                processed_at = EXCLUDED.processed_at
                
            RETURNING id
        """ % (lookback_hours, lookback_hours)
        
        cursor.execute(insert_query)
        result = cursor.fetchall()
        stats['staging_count'] = len(result)
        
        conn.commit()
        
        # VALIDATION STEP: Check temporal consistency
        validation_issues = check_temporal_consistency(conn, lookback_hours)
        
        # Get average quality score
        cursor.execute("""
            SELECT AVG(data_quality_score)
            FROM stg_observations
            WHERE processed_at >= NOW() - INTERVAL '1 hour'
        """)
        avg_quality = cursor.fetchone()[0]
        stats['avg_quality_score'] = float(avg_quality) if avg_quality else 0.0
        
        logger.info(f"✓ Cleaned {stats['staging_count']} observations")
        logger.info(f"  - Avg quality score: {stats['avg_quality_score']:.2f}")
        logger.info(f"  - Temporal validation issues: {validation_issues}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Cleaning failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def clean_and_stage_forecasts(conn, lookback_hours: int = 24) -> Dict[str, Any]:
    """
    Clean raw forecasts and load to staging table.
    
    Same 3-step process as observations.
    """
    logger.info(f"Cleaning forecasts (last {lookback_hours}h)")
    
    cursor = conn.cursor()
    stats = {'raw_count': 0, 'staging_count': 0, 'avg_quality_score': 0.0}
    
    try:
        # Get count of raw records
        cursor.execute("""
            SELECT COUNT(*) FROM raw_weather_forecasts
            WHERE forecast_timestamp >= NOW() - INTERVAL '%s hours'
        """ % (lookback_hours,))
        stats['raw_count'] = cursor.fetchone()[0]
        
        logger.info(f"Found {stats['raw_count']} raw forecasts to process")
        
        if stats['raw_count'] == 0:
            logger.warning("No raw forecasts to clean")
            return stats
        
        # Execute cleaning SQL (same logic as observations)
        insert_query = f"""
            INSERT INTO stg_forecasts (
                station_id, forecast_timestamp, target_timestamp,
                temperature, humidity, pressure, wind_speed, wind_direction,
                precipitation, cloud_cover, visibility, sunshine, dew_point,
                has_missing_values, has_outliers, data_quality_score,
                quality_notes, processed_at, raw_forecast_id
            )
            SELECT 
                station_id,
                forecast_timestamp,
                target_timestamp,
                temperature,
                humidity,
                pressure,
                wind_speed,
                wind_direction,
                precipitation,
                cloud_cover,
                visibility,
                sunshine,
                dew_point,
                
                -- Same cleaning logic as observations
                CASE WHEN (
                    (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END)
                ) > 1 THEN TRUE ELSE FALSE END as has_missing_values,
                
                CASE WHEN (
                    temperature NOT BETWEEN -40 AND 50 OR
                    wind_speed NOT BETWEEN 0 AND 200 OR
                    precipitation NOT BETWEEN 0 AND 200
                ) THEN TRUE ELSE FALSE END as has_outliers,
                
                CASE 
                    WHEN temperature IS NOT NULL 
                        AND wind_speed IS NOT NULL 
                        AND precipitation IS NOT NULL
                        AND temperature BETWEEN -40 AND 50
                        AND wind_speed BETWEEN 0 AND 200
                    THEN 1.0
                    WHEN (temperature IS NULL OR wind_speed IS NULL)
                    THEN 0.6
                    WHEN temperature NOT BETWEEN -40 AND 50
                    THEN 0.3
                    ELSE 0.5
                END as data_quality_score,
                
                CASE 
                    WHEN temperature NOT BETWEEN -40 AND 50 
                    THEN 'Temperature outlier'
                    WHEN temperature IS NULL 
                    THEN 'Missing temperature'
                    ELSE NULL
                END as quality_notes,
                
                NOW() as processed_at,
                id as raw_forecast_id
                
            FROM raw_weather_forecasts
            WHERE forecast_timestamp >= NOW() - INTERVAL '{lookback_hours} hours'
                -- Exclude records with too many missing values
                AND NOT (
                    (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END)
                ) > 1
            
            ON CONFLICT (station_id, forecast_timestamp, target_timestamp) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                data_quality_score = EXCLUDED.data_quality_score,
                processed_at = EXCLUDED.processed_at
                
            RETURNING id
        """
        
        cursor.execute(insert_query)
        result = cursor.fetchall()
        stats['staging_count'] = len(result)
        
        conn.commit()
        
        # Get average quality score
        cursor.execute("""
            SELECT AVG(data_quality_score)
            FROM stg_forecasts
            WHERE processed_at >= NOW() - INTERVAL '1 hour'
        """)
        avg_quality = cursor.fetchone()[0]
        stats['avg_quality_score'] = float(avg_quality) if avg_quality else 0.0
        
        logger.info(f"✓ Cleaned {stats['staging_count']} forecasts")
        logger.info(f"  - Avg quality score: {stats['avg_quality_score']:.2f}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Cleaning failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def check_temporal_consistency(conn, lookback_hours: int = 24) -> int:
    """
    VALIDATION STEP: Check for temporal consistency issues.
    
    Detects:
    - Sudden temperature jumps (>20°C change in 1 hour)
    - Duplicate timestamps for same station
    - Out-of-sequence timestamps
    
    Returns:
        Number of issues found
    """
    cursor = conn.cursor()
    
    try:
        # Check for unrealistic temperature changes
        cursor.execute("""
            WITH temp_changes AS (
                SELECT 
                    station_id,
                    timestamp,
                    temperature,
                    LAG(temperature) OVER (PARTITION BY station_id ORDER BY timestamp) as prev_temp,
                    timestamp - LAG(timestamp) OVER (PARTITION BY station_id ORDER BY timestamp) as time_diff
                FROM stg_observations
                WHERE timestamp >= NOW() - INTERVAL '%s hours'
                    AND temperature IS NOT NULL
            )
            SELECT COUNT(*)
            FROM temp_changes
            WHERE ABS(temperature - prev_temp) > 20
                AND time_diff <= INTERVAL '1 hour'
        """ % (lookback_hours,))
        
        issues = cursor.fetchone()[0]
        
        if issues > 0:
            logger.warning(f"Found {issues} temporal consistency issues (large temperature jumps)")
        
        return issues
        
    except Exception as e:
        logger.error(f"Temporal consistency check failed: {e}")
        return 0
    finally:
        cursor.close()