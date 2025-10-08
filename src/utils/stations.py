"""
Weather station discovery and management utilities.
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class WeatherStationManager:
    """Manage weather station metadata."""
    
    def __init__(self, conn):
        self.conn = conn
    
    def get_active_stations(self, source_type: str = 'observation') -> List[Dict]:
        """
        Get list of active weather stations for a region.
        
        For production: Query stations within Berlin/Brandenburg bounding box
        """
        cursor = self.conn.cursor()
        
        # Berlin/Brandenburg bounding box
        cursor.execute("""
            SELECT 
                station_id,
                latitude,
                longitude,
                station_name
            FROM weather_stations
            WHERE latitude BETWEEN 51.5 AND 53.5
              AND longitude BETWEEN 11.0 AND 15.0
              AND (source = %s OR source IS NULL)
            ORDER BY station_id
        """, (source_type,))
        
        stations = []
        for row in cursor.fetchall():
            stations.append({
                'station_id': row[0],
                'latitude': row[1],
                'longitude': row[2],
                'station_name': row[3]
            })
        
        cursor.close()
        return stations
    
    def upsert_station(self, station_data: Dict) -> bool:
        """Insert or update station metadata."""
        cursor = self.conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO weather_stations (
                    station_id, station_name, latitude, longitude,
                    altitude, source, location, first_record_date, last_record_date
                ) VALUES (
                    %(station_id)s, %(station_name)s, %(latitude)s, %(longitude)s,
                    %(elevation)s, 'brightsky',
                    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
                    %(first_observed_at)s, %(last_observed_at)s
                )
                ON CONFLICT (station_id) 
                DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    altitude = EXCLUDED.altitude,
                    location = EXCLUDED.location,
                    last_record_date = GREATEST(weather_stations.last_record_date, EXCLUDED.last_record_date),
                    updated_at = CURRENT_TIMESTAMP
            """, station_data)
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert station {station_data.get('station_id')}: {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()


# Legacy functions for backward compatibility
def discover_stations_from_data(conn, source_type: str = 'observation') -> int:
    """
    Legacy function - use WeatherStationManager instead.
    Discover weather stations from raw data and update station metadata.
    """
    manager = WeatherStationManager(conn)
    
    try:
        cursor = conn.cursor()
        
        if source_type == 'observation':
            # Get stations from raw_weather_observations
            cursor.execute("""
                SELECT DISTINCT station_id 
                FROM raw_weather_observations 
                WHERE station_id NOT IN (
                    SELECT station_id FROM weather_stations
                )
                LIMIT 100
            """)
        elif source_type == 'forecast':
            # Get stations from raw_weather_forecasts
            cursor.execute("""
                SELECT DISTINCT station_id 
                FROM raw_weather_forecasts 
                WHERE station_id NOT IN (
                    SELECT station_id FROM weather_stations
                )
                LIMIT 100
            """)
        else:
            logger.warning(f"Unknown source_type: {source_type}")
            return 0
            
        new_station_ids = [row[0] for row in cursor.fetchall()]
        
        if not new_station_ids:
            logger.info(f"No new {source_type} stations to discover")
            return 0
            
        logger.info(f"Found {len(new_station_ids)} new {source_type} stations")
        
        # Insert placeholder station records
        stations_created = 0
        for station_id in new_station_ids:
            station_data = {
                'station_id': station_id,
                'station_name': f"Station {station_id}",
                'latitude': None,
                'longitude': None,
                'elevation': None,
                'first_observed_at': None,
                'last_observed_at': None
            }
            
            if manager.upsert_station(station_data):
                stations_created += 1
        
        logger.info(f"Inserted/updated {stations_created} {source_type} stations")
        return stations_created
        
    except Exception as e:
        logger.error(f"Failed to discover stations: {e}")
        return 0
    finally:
        cursor.close()


def get_stations_in_region(conn, lat_min: float, lat_max: float, 
                          lon_min: float, lon_max: float) -> List[Dict]:
    """
    Get all weather stations within a geographic region.
    
    Args:
        conn: Database connection
        lat_min, lat_max: Latitude bounds
        lon_min, lon_max: Longitude bounds
        
    Returns:
        List of station dictionaries
    """
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT station_id, station_name, latitude, longitude, altitude,
                   first_record_date, last_record_date
            FROM weather_stations
            WHERE latitude BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
            ORDER BY station_name
        """, (lat_min, lat_max, lon_min, lon_max))
        
        stations = []
        for row in cursor.fetchall():
            stations.append({
                'station_id': row[0],
                'station_name': row[1],
                'latitude': row[2],
                'longitude': row[3],
                'altitude': row[4],
                'first_record_date': row[5],
                'last_record_date': row[6]
            })
            
        return stations
        
    except Exception as e:
        logger.error(f"Failed to get stations in region: {e}")
        return []
    finally:
        cursor.close()
