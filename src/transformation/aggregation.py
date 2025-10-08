"""
FIXED: Spatial aggregation aligned with actual database schema.

Key fixes:
1. Uses forecast_timestamp (not issue_time) to match schema
2. Correct JOIN order (data → postal codes, not postal codes → data)
3. Proper CONFLICT clause matching actual UNIQUE constraint
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def aggregate_to_postal_codes(
    conn, 
    source_table: str,
    target_table: str,
    data_type: str,
    lookback_hours: int = 24
) -> int:
    """
    FIXED: Aggregate with correct JOIN order and schema alignment.
    
    Schema-aligned fields:
    - analytics_weather_by_postal_code.forecast_timestamp (for forecasts)
    - UNIQUE constraint: (postal_code, timestamp, data_type, forecast_timestamp)
    """
    logger.info(f"Aggregating {source_table} to postal codes (FIXED + schema-aligned)")
    
    cursor = conn.cursor()
    
    try:
        # Configure based on source type
        if source_table == 'stg_observations':
            time_field = 'timestamp'
            time_filter = f"stg.{time_field} >= NOW() - INTERVAL '{lookback_hours} hours'"
            forecast_timestamp_select = 'NULL::TIMESTAMP as forecast_timestamp'
            group_by_extra = ''
        else:  # stg_forecasts
            time_field = 'target_timestamp'
            time_filter = f"stg.forecast_timestamp >= NOW() - INTERVAL '{lookback_hours} hours'"
            forecast_timestamp_select = 'stg.forecast_timestamp as forecast_timestamp'
            group_by_extra = ', stg.forecast_timestamp'
        
        # Check source data
        cursor.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT station_id)
            FROM {source_table} stg
            WHERE {time_filter}
        """)
        source_count, source_stations = cursor.fetchone()
        
        logger.info(f"Source: {source_count} records from {source_stations} stations")
        
        if source_count == 0:
            logger.warning("No source data to aggregate")
            return 0
        
        # FIXED AGGREGATION QUERY (schema-aligned)
        aggregate_query = f"""
        WITH data_with_location AS (
            -- Step 1: Join data to stations for coordinates
            SELECT 
                stg.*,
                ws.location
            FROM {source_table} stg
            JOIN weather_stations ws ON stg.station_id = ws.station_id
            WHERE {time_filter}
              AND stg.data_quality_score >= 0.5
              AND ws.location IS NOT NULL
        ),
        data_to_postal_mapping AS (
            -- Step 2: Map each data point to nearby postal codes
            -- KEY FIX: Start with data, not postal codes!
            SELECT 
                d.station_id,
                d.{time_field},
                {forecast_timestamp_select.replace('stg.', 'd.')},
                d.temperature,
                d.humidity,
                d.pressure,
                d.wind_speed,
                d.wind_direction,
                d.precipitation,
                d.cloud_cover,
                d.visibility,
                d.sunshine,
                d.dew_point,
                d.data_quality_score,
                pc.postal_code,
                ST_Distance(
                    d.location::geography,
                    ST_Centroid(pc.geometry)::geography
                ) / 1000.0 as distance_km,
                -- IDW weight: 1/distance²
                1.0 / NULLIF(POWER(
                    ST_Distance(
                        d.location::geography,
                        ST_Centroid(pc.geometry)::geography
                    ) / 1000.0,
                    2
                ), 0) as idw_weight
            FROM data_with_location d
            JOIN postal_codes pc ON ST_DWithin(
                d.location::geography,
                ST_Centroid(pc.geometry)::geography,
                50000  -- 50km
            )
            WHERE pc.geometry IS NOT NULL
        ),
        hourly_aggregates AS (
            -- Step 3: Aggregate to hourly per postal code
            SELECT 
                postal_code,
                DATE_TRUNC('hour', {time_field}) as hour,
                forecast_timestamp,
                
                -- Temperature with IDW
                SUM(temperature * idw_weight) / NULLIF(SUM(idw_weight), 0) as temperature_avg,
                MIN(temperature) as temperature_min,
                MAX(temperature) as temperature_max,
                
                -- Precipitation (sum for accumulation)
                SUM(precipitation * idw_weight) / NULLIF(SUM(idw_weight), 0) as precipitation_sum,
                
                -- Wind with IDW
                SUM(wind_speed * idw_weight) / NULLIF(SUM(idw_weight), 0) as wind_speed_avg,
                MAX(wind_speed) as wind_speed_max,
                
                -- Circular mean for wind direction
                DEGREES(ATAN2(
                    SUM(SIN(RADIANS(wind_direction)) * idw_weight),
                    SUM(COS(RADIANS(wind_direction)) * idw_weight)
                )) as wind_direction_avg,
                
                -- Other parameters
                SUM(cloud_cover * idw_weight) / NULLIF(SUM(idw_weight), 0) as cloud_cover_avg,
                SUM(pressure * idw_weight) / NULLIF(SUM(idw_weight), 0) as pressure_msl_avg,
                SUM(sunshine * idw_weight) / NULLIF(SUM(idw_weight), 0) as sunshine_sum,
                SUM(visibility * idw_weight) / NULLIF(SUM(idw_weight), 0) as visibility_avg,
                SUM(dew_point * idw_weight) / NULLIF(SUM(idw_weight), 0) as dew_point_avg,
                SUM(humidity * idw_weight) / NULLIF(SUM(idw_weight), 0) as relative_humidity_avg,
                
                -- Quality metrics
                COUNT(DISTINCT station_id) as num_stations,
                AVG(data_quality_score) as avg_quality_score,
                MAX(distance_km) as max_distance_km,
                AVG(distance_km) as avg_distance_km
                
            FROM data_to_postal_mapping
            GROUP BY postal_code, DATE_TRUNC('hour', {time_field}), forecast_timestamp
            HAVING COUNT(*) >= 1
        )
        INSERT INTO {target_table} (
            postal_code, timestamp, data_type, forecast_timestamp,
            temperature_avg, temperature_min, temperature_max,
            precipitation_sum, wind_speed_avg, wind_speed_max,
            wind_direction_avg, cloud_cover_avg, pressure_msl_avg,
            sunshine_sum, visibility_avg, dew_point_avg, relative_humidity_avg,
            num_stations, avg_quality_score, max_distance_km,
            created_at, updated_at
        )
        SELECT 
            postal_code,
            hour as timestamp,
            '{data_type}' as data_type,
                forecast_timestamp,
            ROUND(temperature_avg::NUMERIC, 2),
            ROUND(temperature_min::NUMERIC, 2),
            ROUND(temperature_max::NUMERIC, 2),
            ROUND(precipitation_sum::NUMERIC, 2),
            ROUND(wind_speed_avg::NUMERIC, 2),
            ROUND(wind_speed_max::NUMERIC, 2),
            -- Normalize wind direction to 0-360
            ROUND(
                CASE 
                    WHEN wind_direction_avg < 0 THEN wind_direction_avg + 360
                    WHEN wind_direction_avg >= 360 THEN wind_direction_avg - 360
                    ELSE wind_direction_avg
                END::NUMERIC, 
                0
            )::INTEGER,
            ROUND(cloud_cover_avg::NUMERIC, 0)::INTEGER,
            ROUND(pressure_msl_avg::NUMERIC, 2),
            ROUND(sunshine_sum::NUMERIC, 0)::INTEGER,
            ROUND(visibility_avg::NUMERIC, 0)::INTEGER,
            ROUND(dew_point_avg::NUMERIC, 2),
            ROUND(relative_humidity_avg::NUMERIC, 0)::INTEGER,
            num_stations,
            -- Combined quality score
            ROUND(
                (avg_quality_score * 0.6 +
                 LEAST(num_stations::NUMERIC / 3.0, 1.0) * 0.2 +
                 (1.0 - LEAST(avg_distance_km / 50.0, 1.0)) * 0.2
                )::NUMERIC,
                2
            ) as avg_quality_score,
            ROUND(max_distance_km::NUMERIC, 2),
            NOW(),
            NOW()
        FROM hourly_aggregates
        
        -- Schema-aligned CONFLICT clause
        ON CONFLICT (postal_code, timestamp, data_type, forecast_timestamp) DO UPDATE SET
            temperature_avg = EXCLUDED.temperature_avg,
            temperature_min = EXCLUDED.temperature_min,
            temperature_max = EXCLUDED.temperature_max,
            precipitation_sum = EXCLUDED.precipitation_sum,
            wind_speed_avg = EXCLUDED.wind_speed_avg,
            wind_speed_max = EXCLUDED.wind_speed_max,
            wind_direction_avg = EXCLUDED.wind_direction_avg,
            cloud_cover_avg = EXCLUDED.cloud_cover_avg,
            pressure_msl_avg = EXCLUDED.pressure_msl_avg,
            sunshine_sum = EXCLUDED.sunshine_sum,
            visibility_avg = EXCLUDED.visibility_avg,
            dew_point_avg = EXCLUDED.dew_point_avg,
            relative_humidity_avg = EXCLUDED.relative_humidity_avg,
            num_stations = EXCLUDED.num_stations,
            avg_quality_score = EXCLUDED.avg_quality_score,
            max_distance_km = EXCLUDED.max_distance_km,
            updated_at = NOW()
        """
        
        cursor.execute(aggregate_query)
        records_created = cursor.rowcount
        
        conn.commit()
        
        # Validation
        expansion_factor = records_created / source_count if source_count > 0 else 0
        
        logger.info(f"✓ Aggregation complete:")
        logger.info(f"  Input:  {source_count} records from {source_stations} stations")
        logger.info(f"  Output: {records_created} postal code records")
        logger.info(f"  Expansion: {expansion_factor:.1f}x")
        
        # Sanity checks
        if expansion_factor > 500:
            logger.error(f"❌ UNREALISTIC EXPANSION: {expansion_factor:.1f}x!")
            logger.error("   Data explosion detected - check for CROSS JOIN issues")
        elif expansion_factor < 1:
            logger.warning(f"⚠️  LOW EXPANSION: {expansion_factor:.1f}x")
            logger.warning("   Limited spatial coverage")
        else:
            logger.info(f"  ✅ Expansion factor is reasonable")
        
        # Detailed stats
        if records_created > 0:
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT postal_code) as unique_codes,
                    COUNT(DISTINCT timestamp) as unique_hours,
                    AVG(num_stations) as avg_stations,
                    AVG(avg_quality_score) as avg_quality,
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM {target_table}
                WHERE data_type = '{data_type}'
                    AND updated_at >= NOW() - INTERVAL '5 minutes'
            """)
            
            stats = cursor.fetchone()
            if stats:
                logger.info(f"  Coverage:")
                logger.info(f"    - Postal codes: {stats[0]}")
                logger.info(f"    - Hours: {stats[1]}")
                logger.info(f"    - Time: {stats[4]} to {stats[5]}")
                logger.info(f"  Quality:")
                logger.info(f"    - Avg stations/code: {stats[2]:.1f}")
                logger.info(f"    - Avg quality: {stats[3]:.2f}")
        
        cursor.close()
        return records_created
        
    except Exception as e:
        logger.error(f"Aggregation failed: {e}", exc_info=True)
        conn.rollback()
        cursor.close()
        raise


def diagnose_aggregation(conn, source_table: str) -> Dict[str, Any]:
    """
    Diagnostic to catch data explosion before it happens.
    """
    cursor = conn.cursor()
    
    logger.info("\n" + "="*60)
    logger.info("AGGREGATION DIAGNOSTICS")
    logger.info("="*60)
    
    try:
        # Source data
        if source_table == 'stg_observations':
            time_filter = "timestamp >= NOW() - INTERVAL '24 hours'"
        else:
            time_filter = "forecast_timestamp >= NOW() - INTERVAL '24 hours'"
        
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT station_id) as stations
            FROM {source_table}
            WHERE {time_filter}
        """)
        src_count, src_stations = cursor.fetchone()
        
        logger.info(f"\n1. Source ({source_table}):")
        logger.info(f"   Records: {src_count}")
        logger.info(f"   Stations: {src_stations}")
        
        # Stations with coords
        cursor.execute("""
            SELECT COUNT(*)
            FROM weather_stations
            WHERE location IS NOT NULL
        """)
        stations_with_coords = cursor.fetchone()[0]
        
        # Postal codes
        cursor.execute("""
            SELECT COUNT(*)
            FROM postal_codes
            WHERE geometry IS NOT NULL
        """)
        postal_codes = cursor.fetchone()[0]
        
        logger.info(f"\n2. Spatial Data:")
        logger.info(f"   Stations with coords: {stations_with_coords}")
        logger.info(f"   Postal codes: {postal_codes}")
        
        # Test spatial join (data → postal codes)
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {source_table} stg
            JOIN weather_stations ws ON stg.station_id = ws.station_id
            JOIN postal_codes pc ON ST_DWithin(
                ws.location::geography,
                ST_Centroid(pc.geometry)::geography,
                50000
            )
            WHERE {time_filter}
              AND ws.location IS NOT NULL
              AND pc.geometry IS NOT NULL
        """)
        matches = cursor.fetchone()[0]
        
        expected_expansion = matches / src_count if src_count > 0 else 0
        
        logger.info(f"\n3. Expected Results:")
        logger.info(f"   Spatial matches: {matches}")
        logger.info(f"   Expected expansion: {expected_expansion:.1f}x")
        
        if expected_expansion > 500:
            logger.error(f"   ❌ WILL CAUSE DATA EXPLOSION!")
        elif expected_expansion == 0:
            logger.error(f"   ❌ NO MATCHES - check geometry/coordinates")
        else:
            logger.info(f"   ✅ Looks reasonable")
        
        cursor.close()
        
        return {
            'source_records': src_count,
            'source_stations': src_stations,
            'expected_output': matches,
            'expansion_factor': expected_expansion
        }
        
    except Exception as e:
        logger.error(f"Diagnostics failed: {e}")
        cursor.close()
        return {}

        
        # Check source data count
        cursor.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT station_id)
            FROM {source_table} stg
            WHERE {time_filter}
        """)
        source_count, source_stations = cursor.fetchone()
        
        logger.info(f"Source data: {source_count} records from {source_stations} stations")
        
        if source_count == 0:
            logger.warning("No source data to aggregate")
            return 0
        
        # FIXED AGGREGATION QUERY
        # Key: Start with data, not with postal codes!
        aggregate_query = f"""
        WITH data_with_location AS (
            -- Step 1: Get actual data with station coordinates
            SELECT 
                stg.*,
                ws.latitude,
                ws.longitude,
                ws.location
            FROM {source_table} stg
            JOIN weather_stations ws ON stg.station_id = ws.station_id
            WHERE {time_filter}
              AND stg.data_quality_score >= 0.5
              AND ws.location IS NOT NULL
        ),
        data_to_postal_mapping AS (
            -- Step 2: For each data point, find nearby postal codes
            -- This is the KEY FIX: we go from data → postal codes, not postal codes → data
            SELECT 
                d.station_id,
                d.{time_field},
                {forecast_timestamp_select.replace('stg.', 'd.')},
                d.temperature,
                d.humidity,
                d.pressure,
                d.wind_speed,
                d.wind_direction,
                d.precipitation,
                d.cloud_cover,
                d.visibility,
                d.sunshine,
                d.dew_point,
                d.data_quality_score,
                pc.postal_code,
                ST_Distance(
                    d.location::geography,
                    ST_Centroid(pc.geometry)::geography
                ) / 1000.0 as distance_km,
                -- IDW weight
                1.0 / NULLIF(POWER(
                    ST_Distance(
                        d.location::geography,
                        ST_Centroid(pc.geometry)::geography
                    ) / 1000.0,
                    2
                ), 0) as idw_weight
            FROM data_with_location d
            JOIN postal_codes pc ON ST_DWithin(
                d.location::geography,
                ST_Centroid(pc.geometry)::geography,
                50000  -- 50km radius
            )
            WHERE pc.geometry IS NOT NULL
        ),
        hourly_aggregates AS (
            -- Step 3: Aggregate to hourly buckets per postal code
            SELECT 
                postal_code,
                DATE_TRUNC('hour', {time_field}) as hour,
                forecast_timestamp,
                
                -- Temperature with IDW
                SUM(temperature * idw_weight) / NULLIF(SUM(idw_weight), 0) as temperature_avg,
                MIN(temperature) as temperature_min,
                MAX(temperature) as temperature_max,
                
                -- Other parameters with IDW
                SUM(precipitation * idw_weight) / NULLIF(SUM(idw_weight), 0) as precipitation_sum,
                SUM(wind_speed * idw_weight) / NULLIF(SUM(idw_weight), 0) as wind_speed_avg,
                MAX(wind_speed) as wind_speed_max,
                
                -- Circular mean for wind direction
                DEGREES(ATAN2(
                    SUM(SIN(RADIANS(wind_direction)) * idw_weight),
                    SUM(COS(RADIANS(wind_direction)) * idw_weight)
                )) as wind_direction_avg,
                
                SUM(cloud_cover * idw_weight) / NULLIF(SUM(idw_weight), 0) as cloud_cover_avg,
                SUM(pressure * idw_weight) / NULLIF(SUM(idw_weight), 0) as pressure_msl_avg,
                SUM(sunshine * idw_weight) / NULLIF(SUM(idw_weight), 0) as sunshine_sum,
                SUM(visibility * idw_weight) / NULLIF(SUM(idw_weight), 0) as visibility_avg,
                SUM(dew_point * idw_weight) / NULLIF(SUM(idw_weight), 0) as dew_point_avg,
                SUM(humidity * idw_weight) / NULLIF(SUM(idw_weight), 0) as relative_humidity_avg,
                
                -- Quality tracking
                COUNT(DISTINCT station_id) as num_stations,
                AVG(data_quality_score) as avg_quality_score,
                MAX(distance_km) as max_distance_km,
                AVG(distance_km) as avg_distance_km
                
            FROM data_to_postal_mapping
            GROUP BY postal_code, DATE_TRUNC('hour', {time_field}), forecast_timestamp
            HAVING COUNT(*) >= 1  -- At least one measurement
        )
        INSERT INTO {target_table} (
            postal_code, timestamp, data_type, forecast_timestamp,
            temperature_avg, temperature_min, temperature_max,
            precipitation_sum, wind_speed_avg, wind_speed_max,
            wind_direction_avg, cloud_cover_avg, pressure_msl_avg,
            sunshine_sum, visibility_avg, dew_point_avg, relative_humidity_avg,
            num_stations, avg_quality_score, max_distance_km,
            created_at, updated_at
        )
        SELECT 
            postal_code,
            hour as timestamp,
            '{data_type}' as data_type,
            forecast_timestamp,
            ROUND(temperature_avg::NUMERIC, 2),
            ROUND(temperature_min::NUMERIC, 2),
            ROUND(temperature_max::NUMERIC, 2),
            ROUND(precipitation_sum::NUMERIC, 2),
            ROUND(wind_speed_avg::NUMERIC, 2),
            ROUND(wind_speed_max::NUMERIC, 2),
            ROUND(
                CASE 
                    WHEN wind_direction_avg < 0 THEN wind_direction_avg + 360
                    WHEN wind_direction_avg >= 360 THEN wind_direction_avg - 360
                    ELSE wind_direction_avg
                END::NUMERIC, 
                0
            )::INTEGER,
            ROUND(cloud_cover_avg::NUMERIC, 0)::INTEGER,
            ROUND(pressure_msl_avg::NUMERIC, 2),
            ROUND(sunshine_sum::NUMERIC, 0)::INTEGER,
            ROUND(visibility_avg::NUMERIC, 0)::INTEGER,
            ROUND(dew_point_avg::NUMERIC, 2),
            ROUND(relative_humidity_avg::NUMERIC, 0)::INTEGER,
            num_stations,
            ROUND(
                (avg_quality_score * 0.6 +
                 LEAST(num_stations::NUMERIC / 3.0, 1.0) * 0.2 +
                 (1.0 - LEAST(avg_distance_km / 50.0, 1.0)) * 0.2
                )::NUMERIC,
                2
            ) as avg_quality_score,
            ROUND(max_distance_km::NUMERIC, 2),
            NOW(),
            NOW()
        FROM hourly_aggregates
        
        ON CONFLICT ({conflict_key}) DO UPDATE SET
            temperature_avg = EXCLUDED.temperature_avg,
            temperature_min = EXCLUDED.temperature_min,
            temperature_max = EXCLUDED.temperature_max,
            precipitation_sum = EXCLUDED.precipitation_sum,
            wind_speed_avg = EXCLUDED.wind_speed_avg,
            wind_speed_max = EXCLUDED.wind_speed_max,
            wind_direction_avg = EXCLUDED.wind_direction_avg,
            cloud_cover_avg = EXCLUDED.cloud_cover_avg,
            pressure_msl_avg = EXCLUDED.pressure_msl_avg,
            sunshine_sum = EXCLUDED.sunshine_sum,
            visibility_avg = EXCLUDED.visibility_avg,
            dew_point_avg = EXCLUDED.dew_point_avg,
            relative_humidity_avg = EXCLUDED.relative_humidity_avg,
            num_stations = EXCLUDED.num_stations,
            avg_quality_score = EXCLUDED.avg_quality_score,
            max_distance_km = EXCLUDED.max_distance_km,
            updated_at = EXCLUDED.updated_at
        """
        
        cursor.execute(aggregate_query)
        records_created = cursor.rowcount
        
        conn.commit()
        
        # Validation: Check expansion factor
        expansion_factor = records_created / source_count if source_count > 0 else 0
        
        logger.info(f"✓ Aggregation complete:")
        logger.info(f"  Input:  {source_count} records from {source_stations} stations")
        logger.info(f"  Output: {records_created} postal code records")
        logger.info(f"  Expansion: {expansion_factor:.1f}x")
        
        # Sanity check: expansion should be reasonable
        # Each station can cover multiple postal codes (say 10-100)
        # Each hour creates separate records
        # So expansion of 10-100x is reasonable, 1000x is wrong
        if expansion_factor > 500:
            logger.error(f"❌ UNREALISTIC EXPANSION: {expansion_factor:.1f}x!")
            logger.error("   This suggests a JOIN logic error - check for CROSS JOIN issues")
            logger.error(f"   Expected: ~10-100x, Got: {expansion_factor:.0f}x")
            
            # Show what went wrong
            cursor.execute(f"""
                SELECT COUNT(DISTINCT postal_code), COUNT(DISTINCT timestamp)
                FROM {target_table}
                WHERE updated_at >= NOW() - INTERVAL '5 minutes'
                  AND data_type = '{data_type}'
            """)
            postal_count, hour_count = cursor.fetchone()
            
            expected_expansion = (postal_count * hour_count) / source_count if source_count > 0 else 0
            logger.error(f"   Debug: {postal_count} postal codes × {hour_count} hours ÷ {source_count} records")
            logger.error(f"   Expected expansion: {expected_expansion:.1f}x")
            
        elif expansion_factor < 1:
            logger.warning(f"⚠️  LOW EXPANSION: {expansion_factor:.1f}x (expected ~10-100x)")
            logger.warning("   This suggests limited spatial coverage")
        else:
            logger.info(f"  ✅ Expansion factor looks reasonable")
        
        # Detailed stats
        if records_created > 0:
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT postal_code) as unique_codes,
                    COUNT(DISTINCT timestamp) as unique_hours,
                    AVG(num_stations) as avg_stations,
                    AVG(avg_quality_score) as avg_quality,
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM {target_table}
                WHERE data_type = '{data_type}'
                    AND updated_at >= NOW() - INTERVAL '5 minutes'
            """)
            
            stats = cursor.fetchone()
            if stats:
                logger.info(f"  Coverage:")
                logger.info(f"    - Postal codes: {stats[0]}")
                logger.info(f"    - Time buckets: {stats[1]} hours")
                logger.info(f"    - Time range: {stats[4]} to {stats[5]}")
                logger.info(f"  Quality:")
                logger.info(f"    - Avg stations: {stats[2]:.1f}")
                logger.info(f"    - Avg quality: {stats[3]:.2f}")
        
        cursor.close()
        return records_created
        
    except Exception as e:
        logger.error(f"Aggregation failed: {e}", exc_info=True)
        conn.rollback()
        cursor.close()
        raise


def diagnose_aggregation(conn, source_table: str) -> Dict[str, Any]:
    """
    Diagnostic function to understand data flow and catch issues.
    """
    cursor = conn.cursor()
    
    logger.info("\n" + "="*60)
    logger.info("AGGREGATION DIAGNOSTICS")
    logger.info("="*60)
    
    try:
        # Check 1: Source data
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT station_id) as unique_stations,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM {source_table}
        """)
        src = cursor.fetchone()
        logger.info(f"\n1. Source Data ({source_table}):")
        logger.info(f"   Records: {src[0]}")
        logger.info(f"   Stations: {src[1]}")
        logger.info(f"   Time range: {src[2]} to {src[3]}")
        
        # Check 2: Stations with coordinates
        cursor.execute("""
            SELECT COUNT(*)
            FROM weather_stations
            WHERE location IS NOT NULL
        """)
        stations_with_coords = cursor.fetchone()[0]
        logger.info(f"\n2. Weather Stations:")
        logger.info(f"   With coordinates: {stations_with_coords}")
        
        # Check 3: Postal codes
        cursor.execute("""
            SELECT COUNT(*)
            FROM postal_codes
            WHERE geometry IS NOT NULL
        """)
        postal_with_geom = cursor.fetchone()[0]
        logger.info(f"\n3. Postal Codes:")
        logger.info(f"   With geometry: {postal_with_geom}")
        
        # Check 4: Potential matches (data → postal codes)
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {source_table} stg
            JOIN weather_stations ws ON stg.station_id = ws.station_id
            JOIN postal_codes pc ON ST_DWithin(
                ws.location::geography,
                ST_Centroid(pc.geometry)::geography,
                50000
            )
            WHERE ws.location IS NOT NULL
              AND pc.geometry IS NOT NULL
        """)
        potential_matches = cursor.fetchone()[0]
        
        expected_expansion = potential_matches / src[0] if src[0] > 0 else 0
        
        logger.info(f"\n4. Spatial Matches:")
        logger.info(f"   Potential matches: {potential_matches}")
        logger.info(f"   Expected expansion: {expected_expansion:.1f}x")
        
        if expected_expansion > 500:
            logger.error("   ❌ PROBLEM: Expected expansion > 500x!")
            logger.error("   This will cause data explosion")
        elif expected_expansion < 1:
            logger.error("   ❌ PROBLEM: No spatial matches found!")
        else:
            logger.info("   ✅ Expected expansion looks reasonable")
        
        cursor.close()
        
        return {
            'source_records': src[0],
            'source_stations': src[1],
            'stations_with_coords': stations_with_coords,
            'postal_codes': postal_with_geom,
            'potential_matches': potential_matches,
            'expected_expansion': expected_expansion
        }
        
    except Exception as e:
        logger.error(f"Diagnostics failed: {e}")
        cursor.close()
        return {}