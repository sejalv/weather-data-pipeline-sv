"""
Main transformation pipeline orchestrator.

Coordinates:
1. Data cleaning (removes incomplete/outlier records)
2. Data validation (temporal consistency checks)
3. Spatial aggregation (to postal code level)
"""
import logging
from typing import Dict, Any

from .cleaning import clean_and_stage_observations, clean_and_stage_forecasts
from .aggregation import aggregate_to_postal_codes

logger = logging.getLogger(__name__)


def transform_observations_pipeline(conn, lookback_hours: int = 24) -> Dict[str, Any]:
    """
    Complete transformation pipeline for observations.
    
    Pipeline:
    raw_weather_observations 
      → [cleaning + validation] 
      → stg_observations 
      → [spatial aggregation] 
      → analytics_weather_by_postal_code
    
    Returns:
        Statistics dictionary
    """
    logger.info(f"Starting observations transformation (last {lookback_hours}h)")
    
    stats = {
        'raw_count': 0,
        'staging_count': 0,
        'postal_code_count': 0,
        'avg_quality_score': 0.0
    }
    
    try:
        # Step 1 & 2: Clean and validate → staging
        cleaning_stats = clean_and_stage_observations(conn, lookback_hours)
        
        stats['raw_count'] = cleaning_stats['raw_count']
        stats['staging_count'] = cleaning_stats['staging_count']
        stats['avg_quality_score'] = cleaning_stats['avg_quality_score']
        
        logger.info(f"✓ Cleaning completed: {stats['staging_count']} records in staging")
        
        # Step 3: Aggregate to postal codes
        postal_count = aggregate_to_postal_codes(
            conn,
            source_table='stg_observations',
            target_table='analytics_weather_by_postal_code',
            data_type='observation',
            lookback_hours=lookback_hours
        )
        
        stats['postal_code_count'] = postal_count
        
        logger.info(f"✓ Aggregation completed: {postal_count} postal code records")
        logger.info(f"✅ Observations pipeline completed successfully")
        
        return stats
        
    except Exception as e:
        logger.error(f"Observations pipeline failed: {e}")
        raise


def transform_forecasts_pipeline(conn, lookback_hours: int = 24) -> Dict[str, Any]:
    """
    Complete transformation pipeline for forecasts.
    
    Pipeline:
    raw_weather_forecasts 
      → [cleaning + validation] 
      → stg_forecasts 
      → [spatial aggregation] 
      → analytics_weather_by_postal_code
    
    Returns:
        Statistics dictionary
    """
    logger.info(f"Starting forecasts transformation (last {lookback_hours}h)")
    
    stats = {
        'raw_count': 0,
        'staging_count': 0,
        'postal_code_count': 0,
        'avg_quality_score': 0.0
    }
    
    try:
        # Step 1 & 2: Clean and validate → staging
        cleaning_stats = clean_and_stage_forecasts(conn, lookback_hours)
        
        stats['raw_count'] = cleaning_stats['raw_count']
        stats['staging_count'] = cleaning_stats['staging_count']
        stats['avg_quality_score'] = cleaning_stats['avg_quality_score']
        
        logger.info(f"✓ Cleaning completed: {stats['staging_count']} records in staging")
        
        # Step 3: Aggregate to postal codes
        postal_count = aggregate_to_postal_codes(
            conn,
            source_table='stg_forecasts',
            target_table='analytics_weather_by_postal_code',
            data_type='forecast',
            lookback_hours=lookback_hours
        )
        
        stats['postal_code_count'] = postal_count
        
        logger.info(f"✓ Aggregation completed: {postal_count} postal code records")
        logger.info(f"✅ Forecasts pipeline completed successfully")
        
        return stats
        
    except Exception as e:
        logger.error(f"Forecasts pipeline failed: {e}")
        raise


def validate_ml_ready_data(conn) -> Dict[str, Any]:
    """
    Validate the final ML-ready analytics table.
    
    Checks:
    - Coverage: Are all expected postal codes present?
    - Freshness: Is data recent?
    - Quality: Are quality scores acceptable?
    - Completeness: Are critical fields populated?
    
    Returns:
        Validation results dictionary
    """
    logger.info("Validating ML-ready data...")
    
    cursor = conn.cursor()
    results = {
        'passed': True,
        'issues': [],
        'obs_postal_codes': 0,
        'fcst_postal_codes': 0,
        'obs_quality': 0.0,
        'fcst_quality': 0.0
    }
    
    try:
        # Check 1: Observations coverage
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT postal_code) as unique_codes,
                AVG(avg_quality_score) as avg_quality,
                MAX(timestamp) as latest_timestamp,
                COUNT(*) as total_records
            FROM analytics_weather_by_postal_code
            WHERE data_type = 'observation'
                AND timestamp >= NOW() - INTERVAL '24 hours'
        """)
        
        obs_row = cursor.fetchone()
        if obs_row:
            results['obs_postal_codes'] = obs_row[0] or 0
            results['obs_quality'] = float(obs_row[1]) if obs_row[1] else 0.0
            
            if results['obs_postal_codes'] < 50:  # Lowered threshold for observations
                results['issues'].append(
                    f"Low observations coverage: only {results['obs_postal_codes']} postal codes"
                )
                results['passed'] = False
            
            if results['obs_quality'] < 0.5:  # Lowered quality threshold
                results['issues'].append(
                    f"Low observations quality: {results['obs_quality']:.2f}"
                )
                results['passed'] = False
        else:
            # No observation data found
            results['obs_postal_codes'] = 0
            results['obs_quality'] = 0.0
            results['issues'].append("No observation data found in analytics table")
            results['passed'] = False
        
        # Check 2: Forecasts coverage
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT postal_code) as unique_codes,
                AVG(avg_quality_score) as avg_quality,
                MAX(timestamp) as latest_timestamp,
                COUNT(*) as total_records
            FROM analytics_weather_by_postal_code
            WHERE data_type = 'forecast'
                AND forecast_timestamp >= NOW() - INTERVAL '24 hours'
        """)
        
        fcst_row = cursor.fetchone()
        if fcst_row:
            results['fcst_postal_codes'] = fcst_row[0] or 0
            results['fcst_quality'] = float(fcst_row[1]) if fcst_row[1] else 0.0
            
            if results['fcst_postal_codes'] < 50:  # Lowered threshold for forecasts
                results['issues'].append(
                    f"Low forecasts coverage: only {results['fcst_postal_codes']} postal codes"
                )
                results['passed'] = False
        else:
            # No forecast data found
            results['fcst_postal_codes'] = 0
            results['fcst_quality'] = 0.0
            results['issues'].append("No forecast data found in analytics table")
            results['passed'] = False
        
        # Check 3: Critical fields populated
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE temperature_avg IS NULL) as null_temp,
                COUNT(*) FILTER (WHERE num_stations = 0) as no_stations,
                COUNT(*) as total
            FROM analytics_weather_by_postal_code
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """)
        
        null_check = cursor.fetchone()
        if null_check and null_check[2] > 0:
            null_pct = (null_check[0] / null_check[2]) * 100
            if null_pct > 20:  # Increased threshold to 20%
                results['issues'].append(
                    f"High percentage of null temperatures: {null_pct:.1f}%"
                )
                results['passed'] = False
            
            # Check for records with no stations
            no_stations_pct = (null_check[1] / null_check[2]) * 100
            if no_stations_pct > 10:
                results['issues'].append(
                    f"High percentage of records with no stations: {no_stations_pct:.1f}%"
                )
                results['passed'] = False
        elif null_check and null_check[2] == 0:
            results['issues'].append("No records found in analytics table for validation")
            results['passed'] = False
        
        cursor.close()
        
        # Log detailed results
        logger.info(f"Validation Results:")
        logger.info(f"  Observations: {results['obs_postal_codes']} postal codes, quality: {results['obs_quality']:.2f}")
        logger.info(f"  Forecasts: {results['fcst_postal_codes']} postal codes, quality: {results['fcst_quality']:.2f}")
        
        if results['passed']:
            logger.info("✅ All validation checks passed")
        else:
            logger.warning(f"⚠️  Validation issues found: {len(results['issues'])}")
            for issue in results['issues']:
                logger.warning(f"  - {issue}")
        
        return results
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        cursor.close()
        raise