"""
Weather data endpoints
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os

router = APIRouter()

def get_db_connection():
    """Get database connection"""
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/weather_db")
    return psycopg2.connect(database_url)

@router.get("/postal/{postal_code}")
async def get_weather_by_postal_code(
    postal_code: str,
    data_type: Optional[str] = "observation",
    hours: Optional[int] = 24
):
    """Get weather data for a specific postal code"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
            SELECT 
                postal_code,
                timestamp,
                temperature_avg,
                precipitation_sum,
                wind_speed_avg,
                wind_direction_avg,
                cloud_cover_avg,
                pressure_msl_avg,
                humidity_avg,
                num_stations,
                avg_quality_score,
                max_distance_km
            FROM analytics_weather_by_postal_code
            WHERE postal_code = %s 
              AND data_type = %s
              AND timestamp >= %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            
            start_time = datetime.utcnow() - timedelta(hours=hours)
            cur.execute(query, (postal_code, data_type, start_time, hours))
            results = cur.fetchall()
            
        conn.close()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No weather data found for postal code {postal_code}")
            
        return {
            "postal_code": postal_code,
            "data_type": data_type,
            "records": len(results),
            "data": [dict(row) for row in results]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/latest/{postal_code}")
async def get_latest_weather(postal_code: str):
    """Get latest weather data for a specific postal code"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
            SELECT 
                postal_code,
                timestamp,
                temperature_avg,
                precipitation_sum,
                wind_speed_avg,
                wind_direction_avg,
                cloud_cover_avg,
                pressure_msl_avg,
                humidity_avg,
                num_stations,
                avg_quality_score
            FROM analytics_weather_by_postal_code
            WHERE postal_code = %s 
              AND data_type = 'observation'
            ORDER BY timestamp DESC
            LIMIT 1
            """
            
            cur.execute(query, (postal_code,))
            result = cur.fetchone()
            
        conn.close()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"No weather data found for postal code {postal_code}")
            
        return dict(result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/forecast/{postal_code}")
async def get_forecast(postal_code: str, hours_ahead: Optional[int] = 48):
    """Get weather forecast for a specific postal code"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
            SELECT 
                postal_code,
                timestamp,
                temperature_avg,
                precipitation_sum,
                wind_speed_avg,
                wind_direction_avg,
                cloud_cover_avg,
                pressure_msl_avg,
                humidity_avg,
                num_stations,
                avg_quality_score,
                forecast_timestamp
            FROM analytics_weather_by_postal_code
            WHERE postal_code = %s 
              AND data_type = 'forecast'
              AND timestamp >= NOW()
              AND timestamp <= NOW() + INTERVAL '%s hours'
            ORDER BY timestamp ASC
            """
            
            cur.execute(query, (postal_code, hours_ahead))
            results = cur.fetchall()
            
        conn.close()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No forecast data found for postal code {postal_code}")
            
        return {
            "postal_code": postal_code,
            "forecast_hours": hours_ahead,
            "records": len(results),
            "data": [dict(row) for row in results]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
