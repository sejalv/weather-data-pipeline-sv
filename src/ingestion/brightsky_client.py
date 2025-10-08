"""
BrightSky API Client - Lean implementation with only essential functionality.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import structlog
import time
from random import uniform
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger()


class BrightSkyClient:
    """Minimal client for BrightSky API with retry logic."""

    BASE_URL = "https://api.brightsky.dev"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.client = httpx.Client(timeout=timeout)

    def __enter__(self) -> "BrightSkyClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.client.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def get_weather(
        self,
        lat: float,
        lon: float,
        date: str,
        last_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get weather data for a location and date range.
        
        This single method handles both observations and forecasts.
        BrightSky returns past data as observations, future data as forecasts.
        """
        params = {"lat": lat, "lon": lon, "date": date}
        if last_date:
            params["last_date"] = last_date

        logger.info("fetching_weather", lat=lat, lon=lon, date=date, last_date=last_date)

        try:
            response = self.client.get(f"{self.BASE_URL}/weather", params=params)
            response.raise_for_status()
            data = response.json()

            logger.info(
                "weather_fetched",
                records=len(data.get("weather", [])),
                sources=len(data.get("sources", [])),
            )
            return data

        except httpx.HTTPError as e:
            logger.error("fetch_failed", error=str(e), lat=lat, lon=lon)
            raise


def polite_wait():
    """Request throttling for Bright Sky API across hundreds of postal codes."""
    time.sleep(uniform(0.2, 0.5))


def parse_weather_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Parse weather record to database-ready format."""
    return {
        "timestamp": datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00")),
        "temperature": record.get("temperature"),
        "precipitation": record.get("precipitation"),
        "wind_speed": record.get("wind_speed"),
        "wind_direction": record.get("wind_direction"),
        "cloud_cover": record.get("cloud_cover"),
        "pressure_msl": record.get("pressure_msl"),
        "sunshine": record.get("sunshine"),
        "visibility": record.get("visibility"),
        "dew_point": record.get("dew_point"),
        "relative_humidity": record.get("relative_humidity"),
        "observation_type": record.get("observation_type"),
    }


def parse_station_metadata(source: Dict[str, Any]) -> Dict[str, Any]:
    """Parse station metadata from BrightSky source."""
    station_id = str(source.get("id") or source.get("station_id", "unknown"))
    
    # Handle datetime strings
    first_record = source.get("first_record")
    last_record = source.get("last_record")
    
    if isinstance(first_record, str):
        try:
            first_record = datetime.fromisoformat(first_record.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            first_record = None
    
    if isinstance(last_record, str):
        try:
            last_record = datetime.fromisoformat(last_record.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            last_record = None
    
    return {
        "station_id": station_id,
        "station_name": source.get("station_name") or source.get("wmo_station_id"),
        "latitude": float(source.get("lat")) if source.get("lat") else None,
        "longitude": float(source.get("lon")) if source.get("lon") else None,
        "elevation": float(source.get("height")) if source.get("height") else None,
        "first_observed_at": first_record,
        "last_observed_at": last_record,
    }


# Create a global client instance for convenience
brightsky_client = BrightSkyClient()
