"""
Weather forecasts ingestion from BrightSky API.
"""
import logging
from datetime import datetime, timedelta, timezone
from psycopg2.extras import execute_values

from .brightsky_client import brightsky_client

logger = logging.getLogger(__name__)


def create_stations_from_forecasts(conn, forecasts: list, sources_data: list = None) -> int:
    """
    Create weather stations from forecast data and sources metadata if they don't exist.
    
    Creates stations for all sources with observation_type="forecast", regardless of whether
    they have actual weather data.
    
    Args:
        conn: Database connection
        forecasts: List of forecast dicts from API
        sources_data: List of source metadata from API
        
    Returns:
        Number of stations created
    """
    try:
        cursor = conn.cursor()
        
        station_ids = set()
        station_data = {}
        
        # Process sources data for all forecast observation stations
        if sources_data:
            from .brightsky_client import parse_station_metadata
            for source in sources_data:
                try:
                    # Only create stations for forecast observation types
                    if source.get('observation_type') == 'forecast':
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
        
        # Fallback: extract from forecasts if no sources data
        if not station_data:
            for fcst in forecasts:
                station_id = fcst.get('source_id')
                if station_id:
                    station_id = str(station_id)
                    station_ids.add(station_id)
                    if station_id not in station_data:
                        station_data[station_id] = {
                            'station_id': station_id,
                            'station_name': f"Station {station_id}",
                            'latitude': fcst.get('lat'),
                            'longitude': fcst.get('lon'),
                            'altitude': None,
                            'source': 'brightsky',
                            'first_record_date': fcst.get('timestamp'),
                            'last_record_date': fcst.get('timestamp')
                        }
        
        if not station_ids:
            logger.info("No forecast observation stations found")
            return 0
        
        # Check which stations already exist
        cursor.execute("""
            SELECT station_id FROM weather_stations 
            WHERE station_id = ANY(%s)
        """, ([str(sid) for sid in station_ids],))
        
        existing_stations = {row[0] for row in cursor.fetchall()}
        new_stations = station_ids - existing_stations
        
        if not new_stations:
            logger.info("All forecast stations already exist")
            return 0
        
        logger.info(f"Creating {len(new_stations)} new forecast stations")
        
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
        
        logger.info(f"Created {stations_created} new forecast stations")
        return stations_created
        
    except Exception as e:
        logger.error(f"Failed to create stations from forecasts: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()


def ingest_forecasts(conn, forecast_days: int = 10) -> int:
    """
    Ingest weather forecasts from BrightSky API.
    
    Args:
        conn: Database connection
        forecast_days: Number of days ahead to fetch (default: 10)
        
    Returns:
        Number of records inserted
    """
    try:
        logger.info(f"Starting forecasts ingestion for next {forecast_days} days")
        
        # 1. Define date range (today to +forecast_days)
        start_date = datetime.utcnow().date()
        end_date = start_date + timedelta(days=forecast_days)
        
        # 2. Fetch from API (Berlin center)
        lat, lon = 52.5200, 13.4050
        
        logger.info(f"Fetching forecasts from {start_date} to {end_date}")
        
        data = brightsky_client.get_weather(
            lat=lat,
            lon=lon,
            date=start_date.isoformat(),
            last_date=end_date.isoformat()
        )
        
        weather_records = data.get('weather', [])
        sources_data = data.get('sources', [])
        
        if not weather_records:
            logger.warning("No forecast data returned from API")
            return 0
        
        logger.info(f"Retrieved {len(weather_records)} records from API")
        
        # 3. Filter for forecasts only (future timestamps)
        from datetime import timezone
        now = datetime.now(timezone.utc)  # Make timezone-aware
        forecast_timestamp = now  # When this forecast was created
        forecasts = []
        
        for record in weather_records:
            try:
                timestamp = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                
                # Only keep future data (forecasts)
                if timestamp > now:
                    forecasts.append(record)
            except Exception as e:
                logger.warning(f"Failed to parse timestamp: {e}")
                continue
        
        if not forecasts:
            logger.info("No new forecasts to ingest")
            return 0
        
        logger.info(f"Filtered to {len(forecasts)} forecasts")
        
        # 4. Create stations on-the-fly before inserting forecasts
        create_stations_from_forecasts(conn, forecasts, sources_data)
        
        # 5. Insert to database
        inserted = insert_forecasts_batched(
            conn, 
            forecasts, 
            forecast_timestamp,
            batch_size=100
        )
        
        logger.info(f"Successfully inserted {inserted} forecasts")
        
        return inserted
        
    except Exception as e:
        logger.error(f"Forecasts ingestion failed: {e}")
        raise


def insert_forecasts_batched(
    conn, 
    forecasts: list, 
    forecast_timestamp: datetime,
    batch_size: int = 100
) -> int:
    """Insert forecasts in batches."""
    cursor = conn.cursor()
    total_inserted = 0
    
    insert_query = """
        INSERT INTO raw_weather_forecasts (
            station_id, forecast_timestamp, target_timestamp,
            temperature, precipitation, wind_speed, wind_direction,
            humidity, pressure, cloud_cover, visibility,
            sunshine, dew_point, latitude, longitude,
            created_at, source
        ) VALUES %s
        ON CONFLICT (station_id, forecast_timestamp, target_timestamp) 
        DO NOTHING
    """
    
    for i in range(0, len(forecasts), batch_size):
        batch = forecasts[i:i + batch_size]
        
        try:
            records = []
            for fcst in batch:
                records.append((
                    str(fcst.get('source_id')),  # station_id
                    forecast_timestamp,      # forecast_timestamp (when forecast was made)
                    fcst.get('timestamp'),   # target_timestamp (forecast valid time)
                    fcst.get('temperature'),
                    fcst.get('precipitation'),
                    fcst.get('wind_speed'),
                    fcst.get('wind_direction'),
                    fcst.get('relative_humidity'),
                    fcst.get('pressure_msl'),
                    fcst.get('cloud_cover'),
                    fcst.get('visibility'),
                    fcst.get('sunshine'),
                    fcst.get('dew_point'),
                    float(fcst.get('lat', 0)) if fcst.get('lat') else None,  # latitude
                    float(fcst.get('lon', 0)) if fcst.get('lon') else None,  # longitude
                    datetime.now(timezone.utc),  # created_at
                    'brightsky'  # source
                ))
            
            execute_values(cursor, insert_query, records)
            conn.commit()
            
            total_inserted += len(batch)
            
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
        except Exception as e:
            logger.error(f"Batch {i//batch_size + 1} failed: {e}")
            conn.rollback()
            continue
    
    cursor.close()
    
    return total_inserted