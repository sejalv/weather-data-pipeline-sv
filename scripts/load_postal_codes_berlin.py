#!/usr/bin/env python3
"""
Load German postal codes for Berlin/Brandenburg region using yetzt/postleitzahlen approach.
Uses Overpass API with targeted query for 10xxx-19xxx postal codes.
"""
import sys
import os
import requests
import psycopg2
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_postal_codes():
    try:
        logger.info("Fetching postal code boundaries from OpenStreetMap for Berlin/Brandenburg...")
        overpass_url = "http://overpass-api.de/api/interpreter"
        overpass_query = """
        [out:json][timeout:60];
        (
          relation["postal_code"~"^1[0-9][0-9][0-9][0-9]$"]["type"="boundary"]["boundary"="postal_code"];
          way["postal_code"~"^1[0-9][0-9][0-9][0-9]$"];
        );
        out geom;
        """
        response = requests.post(overpass_url, data=overpass_query, timeout=120)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Found {len(data['elements'])} postal code elements")

        # Use environment variable for database URL if available
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/weather_db')
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        inserted = 0
        for element in data['elements']:
            if element['type'] == 'way' and 'postal_code' in element.get('tags', {}):
                postal_code = element['tags']['postal_code']
                city = element['tags'].get('name', '')
                state = element['tags'].get('state', '')
                if 'geometry' in element and len(element['geometry']) >= 3:
                    coords = [[coord['lon'], coord['lat']] for coord in element['geometry']]
                    if coords[0] != coords[-1]:
                        coords.append(coords[0])
                    geom_json = {"type": "MultiPolygon", "coordinates": [[coords]]}
                    cursor.execute("""
                        INSERT INTO postal_codes (postal_code, city, state, geometry)
                        VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s))
                        ON CONFLICT (postal_code) DO NOTHING
                    """, (postal_code, city, state, json.dumps(geom_json)))
                    inserted += cursor.rowcount
            elif element['type'] == 'relation' and 'postal_code' in element.get('tags', {}):
                postal_code = element['tags']['postal_code']
                city = element['tags'].get('name', '')
                state = element['tags'].get('state', '')
                logger.debug(f"Skipping relation for postal code {postal_code}")
                continue
        conn.commit()
        logger.info(f"Successfully inserted {inserted} postal codes")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to load postal codes: {e}")
        return False

if __name__ == "__main__":
    success = load_postal_codes()
    sys.exit(0 if success else 1)