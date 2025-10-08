"""
Weather observations ingestion from BrightSky API.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values

from .brightsky_client import brightsky_client

logger = logging.getLogger(__name__)


def create_stations_from_observations(conn, observations: list, sources_data: list = None) -> int:
    """
    Create weather stations from observation data and sources metadata if they don't exist.
    
    Creates stations for all sources with observation_type="historical", regardless of whether
    they have actual weather data.
    
    Args:
        conn: Database connection
        observations: List of observation dicts from API
        sources_data: List of source metadata from API
        
    Returns:
        Number of stations created
    """
    try:
        cursor = conn.cursor()
        
        station_ids = set()
        station_data = {}
        
        # Process sources data for all historical observation stations
        if sources_data:
            from .brightsky_client import parse_station_metadata
            for source in sources_data:
                try:
                    # Only create stations for historical observation types
                    if source.get('observation_type') in ["historical", "synop", "current"]:
                        station_meta = parse_station_metadata(source)
                        station_id = station_meta['station_id']
                    
                        station_ids.add(station_id)
                        station_data[station_id] = {
                            'station_id': station_id,
                            'station_name': station_meta.get('station_name', f"Station {station_id}"),
                            'latitude': station_meta.get('latitude'),
                            'longitude': station_meta.get('longitude'),
                            'altitude': station_meta.get('elevation'),
                            'source': 'brightsky',
                            'first_record_date': station_meta.get('first_observed_at'),
                            'last_record_date': station_meta.get('last_observed_at')
                        }
                except Exception as e:
                    logger.warning(f"Failed to parse station metadata: {e}")
                    continue
        
        # Fallback: extract from observations if no sources data
        if not station_data:
            for obs in observations:
                station_id = obs.get('source_id')
                if station_id:
                    station_id = str(station_id)
                    station_ids.add(station_id)
                    if station_id not in station_data:
                        station_data[station_id] = {
                            'station_id': station_id,
                            'station_name': f"Station {station_id}",
                            'latitude': obs.get('lat'),
                            'longitude': obs.get('lon'),
                            'altitude': None,
                            'source': 'brightsky',
                            'first_record_date': obs.get('timestamp'),
                            'last_record_date': obs.get('timestamp')
                        }
        
        if not station_ids:
            logger.info("No historical observation stations found")
            return 0
        
        # Check which stations already exist
        cursor.execute("""
            SELECT station_id FROM weather_stations 
            WHERE station_id = ANY(%s)
        """, ([str(sid) for sid in station_ids],))
        
        existing_stations = {row[0] for row in cursor.fetchall()}
        new_stations = station_ids - existing_stations
        
        if not new_stations:
            logger.info("All stations already exist")
            return 0
        
        logger.info(f"Creating {len(new_stations)} new stations")
        
        # Insert new stations
        stations_to_insert = []
        for station_id in new_stations:
            data = station_data[station_id]
            stations_to_insert.append((
                data['station_id'],
                data['station_name'],
                data['latitude'],
                data['longitude'],
                data['altitude'],
                data['source'],
                f"POINT({data['longitude']} {data['latitude']})" if data['longitude'] and data['latitude'] else None,
                data['first_record_date'],
                data['last_record_date'],
                datetime.utcnow(),
                datetime.utcnow()
            ))
        
        # Insert stations with ON CONFLICT to avoid duplicates
        from psycopg2.extras import execute_values
        execute_values(cursor, """
            INSERT INTO weather_stations (
                station_id, station_name, latitude, longitude, altitude,
                source, location, first_record_date, last_record_date,
                created_at, updated_at
            ) VALUES %s
            ON CONFLICT (station_id) DO UPDATE SET
                last_record_date = GREATEST(weather_stations.last_record_date, EXCLUDED.last_record_date),
                updated_at = EXCLUDED.updated_at
        """, stations_to_insert)
        
        stations_created = cursor.rowcount
        conn.commit()
        
        logger.info(f"Created {stations_created} new stations")
        return stations_created
        
    except Exception as e:
        logger.error(f"Failed to create stations from observations: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()


def get_last_observation_timestamp(conn) -> Optional[datetime]:
    """
    Get the most recent observation timestamp from database.
    Used for incremental loading.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(timestamp) 
        FROM raw_weather_observations
    """)
    result = cursor.fetchone()
    cursor.close()
    
    return result[0] if result and result[0] else None


def ingest_observations_by_type(conn, observations: list, sources_data: list = None) -> int:
    """
    Ingest observations based on observation_type with appropriate validation flags.
    
    Logic:
    - IF observation_type in ['historical']: UPSERT into observations (validated=True)
    - ELSE IF observation_type in ['synop', 'current']: UPSERT into observations (validated=False, source_type=observation_type)
    - ELSE: Log and skip (unknown type)
    
    Args:
        conn: Database connection
        observations: List of observation dicts from API
        sources_data: List of source metadata from API
        
    Returns:
        Number of records inserted
    """
    try:
        cursor = conn.cursor()
        total_ingested = 0
        
        # Create a mapping of source_id to observation_type
        source_type_map = {}
        if sources_data:
            for source in sources_data:
                source_id = str(source.get('id', ''))
                obs_type = source.get('observation_type', 'unknown')
                source_type_map[source_id] = obs_type
        
        # Group observations by type
        historical_obs = []
        synop_obs = []
        current_obs = []
        unknown_obs = []
        
        for obs in observations:
            source_id = str(obs.get('source_id', ''))
            obs_type = source_type_map.get(source_id, 'unknown')
            
            if obs_type == 'historical':
                historical_obs.append(obs)
            elif obs_type in ['synop', 'current']:
                if obs_type == 'synop':
                    synop_obs.append(obs)
                else:
                    current_obs.append(obs)
            else:
                unknown_obs.append(obs)
                logger.warning(f"Unknown observation_type '{obs_type}' for station {source_id}, skipping")
        
        logger.info(f"Grouped observations: historical={len(historical_obs)}, synop={len(synop_obs)}, current={len(current_obs)}, unknown={len(unknown_obs)}")
        
        # Upsert historical observations (validated=True)
        if historical_obs:
            ingested = upsert_observations_with_flags(conn, historical_obs, validated=True, source_type='historical')
            total_ingested += ingested
            logger.info(f"Ingested {ingested} historical observations (validated=True)")
        
        # Upsert synop observations (validated=False, source_type=synop)
        if synop_obs:
            ingested = upsert_observations_with_flags(conn, synop_obs, validated=False, source_type='synop')
            total_ingested += ingested
            logger.info(f"Ingested {ingested} synop observations (validated=False)")
        
        # Upsert current observations (validated=False, source_type=current)
        if current_obs:
            ingested = upsert_observations_with_flags(conn, current_obs, validated=False, source_type='current')
            total_ingested += ingested
            logger.info(f"Ingested {ingested} current observations (validated=False)")
        
        conn.commit()
        return total_ingested
        
    except Exception as e:
        logger.error(f"Failed to ingest observations by type: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()


def upsert_observations_with_flags(conn, observations: list, validated: bool, source_type: str, batch_size: int = 100) -> int:
    """
    Insert observations with validation and source_type flags.
    
    Args:
        conn: Database connection
        observations: List of observation dicts
        validated: Validation flag (True for historical, False for synop/current)
        source_type: Source type (historical, synop, current)
        batch_size: Records per batch
        
    Returns:
        Number of records inserted
    """
    cursor = conn.cursor()
    total_inserted = 0
    
    insert_query = """
        INSERT INTO raw_weather_observations (
            station_id, timestamp,
            temperature, precipitation, wind_speed, wind_direction,
            humidity, pressure, cloud_cover, visibility,
            sunshine, dew_point, latitude, longitude,
            validated, source_type, created_at, source
        ) VALUES %s
        ON CONFLICT (station_id, timestamp) 
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            precipitation = EXCLUDED.precipitation,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            humidity = EXCLUDED.humidity,
            pressure = EXCLUDED.pressure,
            cloud_cover = EXCLUDED.cloud_cover,
            visibility = EXCLUDED.visibility,
            sunshine = EXCLUDED.sunshine,
            dew_point = EXCLUDED.dew_point,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            validated = EXCLUDED.validated,
            source_type = EXCLUDED.source_type,
            updated_at = CURRENT_TIMESTAMP
    """
    
    for i in range(0, len(observations), batch_size):
        batch = observations[i:i + batch_size]
        
        try:
            records = []
            for obs in batch:
                records.append((
                    str(obs.get('source_id')),  # station_id
                    obs.get('timestamp'),       # timestamp
                    obs.get('temperature'),     # temperature
                    obs.get('precipitation'),   # precipitation
                    obs.get('wind_speed'),      # wind_speed
                    obs.get('wind_direction'),  # wind_direction
                    obs.get('humidity'),        # humidity
                    obs.get('pressure'),        # pressure
                    obs.get('cloud_cover'),     # cloud_cover
                    obs.get('visibility'),      # visibility
                    obs.get('sunshine'),        # sunshine
                    obs.get('dew_point'),       # dew_point
                    obs.get('lat'),             # latitude
                    obs.get('lon'),             # longitude
                    validated,                  # validated flag
                    source_type,                # source_type
                    datetime.utcnow(),          # created_at
                    'brightsky'                 # source
                ))
            
            execute_values(cursor, insert_query, records)
            total_inserted += cursor.rowcount
            
        except Exception as e:
            logger.error(f"Failed to insert batch {i//batch_size + 1}: {e}")
            continue
    
    return total_inserted

def ingest_observations(conn, lookback_days: int = 7) -> int:
    """
    Ingest weather observations from BrightSky API.
    
    Uses incremental loading - only fetches data since last ingestion.
    
    Args:
        conn: Database connection
        lookback_days: Fallback days if no previous data (default: 7)
        
    Returns:
        Number of records inserted
    """
    try:
        logger.info("Starting observations ingestion")
        
        # 1. Determine date range (incremental loading)
        last_timestamp = get_last_observation_timestamp(conn)
        
        if last_timestamp:
            # Incremental: start from last timestamp
            start_date = last_timestamp.date()
            logger.info(f"Incremental mode: fetching from {start_date}")
        else:
            # First run: fetch lookback period
            start_date = (datetime.utcnow() - timedelta(days=lookback_days)).date()
            logger.info(f"Initial load: fetching last {lookback_days} days")
        
        end_date = datetime.utcnow().date()
        
        # 2. Fetch data from BrightSky API
        # Using Berlin center coordinates
        lat, lon = 52.5200, 13.4050
        
        logger.info(f"Fetching observations from {start_date} to {end_date}")
        
        data = brightsky_client.get_weather(
            lat=lat,
            lon=lon,
            date=start_date.isoformat(),
            last_date=end_date.isoformat()
        )
        
        weather_records = data.get('weather', [])
        sources_data = data.get('sources', [])
        
        if not weather_records:
            logger.warning("No weather data returned from API")
            return 0
        
        logger.info(f"Retrieved {len(weather_records)} records from API")
        
        # 3. Filter for observations only (past timestamps)
        from datetime import timezone
        now = datetime.now(timezone.utc)  # Make timezone-aware
        observations = []
        
        for record in weather_records:
            try:
                timestamp = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                
                # Only keep past data (observations)
                if timestamp < now:
                    observations.append(record)
            except Exception as e:
                logger.warning(f"Failed to parse timestamp: {e}")
                continue
        
        if not observations:
            logger.info("No new observations to ingest")
            return 0
        
        logger.info(f"Filtered to {len(observations)} observations")
        
        # 4. Create stations on-the-fly before inserting observations
        create_stations_from_observations(conn, observations, sources_data)
        
        # 5. Insert to database using observation_type logic
        inserted = ingest_observations_by_type(conn, observations, sources_data)
        
        logger.info(f"Successfully inserted {inserted} observations")
        
        return inserted
        
    except Exception as e:
        logger.error(f"Observations ingestion failed: {e}")
        raise


def upsert_observations_batched(conn, observations: list, batch_size: int = 100) -> int:
    """
    Insert observations in batches with UPSERT for idempotency.
    
    Args:
        conn: Database connection
        observations: List of observation dicts from API
        batch_size: Records per batch (default: 100)
        
    Returns:
        Total number of records processed
    """
    cursor = conn.cursor()
    total_inserted = 0
    
    insert_query = """
        INSERT INTO raw_weather_observations (
            station_id, timestamp,
            temperature, precipitation, wind_speed, wind_direction,
            humidity, pressure, cloud_cover, visibility,
            sunshine, dew_point, latitude, longitude,
            created_at, source
        ) VALUES %s
        ON CONFLICT (station_id, timestamp) 
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            precipitation = EXCLUDED.precipitation,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            humidity = EXCLUDED.humidity,
            pressure = EXCLUDED.pressure,
            cloud_cover = EXCLUDED.cloud_cover,
            visibility = EXCLUDED.visibility,
            sunshine = EXCLUDED.sunshine,
            dew_point = EXCLUDED.dew_point,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            source = EXCLUDED.source,
            updated_at = CURRENT_TIMESTAMP
    """
    
    # Process in batches
    for i in range(0, len(observations), batch_size):
        batch = observations[i:i + batch_size]
        
        try:
            # Parse records
            records = []
            for obs in batch:
                records.append((
                    str(obs.get('source_id')),  # station_id
                    obs.get('timestamp'),   # timestamp
                    obs.get('temperature'),
                    obs.get('precipitation'),
                    obs.get('wind_speed'),
                    obs.get('wind_direction'),
                    obs.get('relative_humidity'),
                    obs.get('pressure_msl'),
                    obs.get('cloud_cover'),
                    obs.get('visibility'),
                    obs.get('sunshine'),
                    obs.get('dew_point'),
                    float(obs.get('lat', 0)) if obs.get('lat') else None,
                    float(obs.get('lon', 0)) if obs.get('lon') else None,
                    datetime.utcnow(),
                    'brightsky'  # source
                ))
            
            # Execute batch insert
            execute_values(cursor, insert_query, records)
            conn.commit()
            
            total_inserted += len(batch)
            
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
        except Exception as e:
            logger.error(f"Batch {i//batch_size + 1} failed: {e}")
            conn.rollback()
            # Continue with next batch instead of failing completely
            continue
    
    cursor.close()
    
    return total_inserted