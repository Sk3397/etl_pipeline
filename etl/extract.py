"""
extract.py - Data extraction from public APIs
Fetches weather data from Open-Meteo API for multiple cities worldwide.
"""

import requests
import pandas as pd
from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)

CITIES = [
    {"name": "New York",     "country": "US", "lat": 40.7128,  "lon": -74.0060},
    {"name": "London",       "country": "GB", "lat": 51.5074,  "lon": -0.1278},
    {"name": "Tokyo",        "country": "JP", "lat": 35.6762,  "lon": 139.6503},
    {"name": "Paris",        "country": "FR", "lat": 48.8566,  "lon": 2.3522},
    {"name": "Sydney",       "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"name": "Mumbai",       "country": "IN", "lat": 19.0760,  "lon": 72.8777},
    {"name": "Bangalore",    "country": "IN", "lat": 12.9716,  "lon": 77.5946},
    {"name": "Delhi",        "country": "IN", "lat": 28.6139,  "lon": 77.2090},
    {"name": "Chicago",      "country": "US", "lat": 41.8781,  "lon": -87.6298},
    {"name": "Los Angeles",  "country": "US", "lat": 34.0522,  "lon": -118.2437},
    {"name": "Toronto",      "country": "CA", "lat": 43.6532,  "lon": -79.3832},
    {"name": "Berlin",       "country": "DE", "lat": 52.5200,  "lon": 13.4050},
    {"name": "Singapore",    "country": "SG", "lat": 1.3521,   "lon": 103.8198},
    {"name": "Dubai",        "country": "AE", "lat": 25.2048,  "lon": 55.2708},
    {"name": "Sao Paulo",    "country": "BR", "lat": -23.5505, "lon": -46.6333},
    {"name": "Mexico City",  "country": "MX", "lat": 19.4326,  "lon": -99.1332},
    {"name": "Cairo",        "country": "EG", "lat": 30.0444,  "lon": 31.2357},
    {"name": "Istanbul",     "country": "TR", "lat": 41.0082,  "lon": 28.9784},
    {"name": "Moscow",       "country": "RU", "lat": 55.7558,  "lon": 37.6176},
    {"name": "Beijing",      "country": "CN", "lat": 39.9042,  "lon": 116.4074},
    {"name": "Shanghai",     "country": "CN", "lat": 31.2304,  "lon": 121.4737},
    {"name": "Seoul",        "country": "KR", "lat": 37.5665,  "lon": 126.9780},
    {"name": "Bangkok",      "country": "TH", "lat": 13.7563,  "lon": 100.5018},
    {"name": "Lagos",        "country": "NG", "lat": 6.5244,   "lon": 3.3792},
    {"name": "Nairobi",      "country": "KE", "lat": -1.2921,  "lon": 36.8219},
    {"name": "Johannesburg", "country": "ZA", "lat": -26.2041, "lon": 28.0473},
    {"name": "Buenos Aires", "country": "AR", "lat": -34.6037, "lon": -58.3816},
    {"name": "Lima",         "country": "PE", "lat": -12.0464, "lon": -77.0428},
    {"name": "Bogota",       "country": "CO", "lat": 4.7110,   "lon": -74.0721},
    {"name": "Karachi",      "country": "PK", "lat": 24.8607,  "lon": 67.0011},
    {"name": "Dhaka",        "country": "BD", "lat": 23.8103,  "lon": 90.4125},
    {"name": "Jakarta",      "country": "ID", "lat": -6.2088,  "lon": 106.8456},
    {"name": "Manila",       "country": "PH", "lat": 14.5995,  "lon": 120.9842},
    {"name": "Osaka",        "country": "JP", "lat": 34.6937,  "lon": 135.5023},
    {"name": "Houston",      "country": "US", "lat": 29.7604,  "lon": -95.3698},
    {"name": "Phoenix",      "country": "US", "lat": 33.4484,  "lon": -112.0740},
    {"name": "Amsterdam",    "country": "NL", "lat": 52.3676,  "lon": 4.9041},
    {"name": "Madrid",       "country": "ES", "lat": 40.4168,  "lon": -3.7038},
    {"name": "Rome",         "country": "IT", "lat": 41.9028,  "lon": 12.4964},
    {"name": "Vienna",       "country": "AT", "lat": 48.2082,  "lon": 16.3738},
    {"name": "Zurich",       "country": "CH", "lat": 47.3769,  "lon": 8.5417},
    {"name": "Stockholm",    "country": "SE", "lat": 59.3293,  "lon": 18.0686},
    {"name": "Oslo",         "country": "NO", "lat": 59.9139,  "lon": 10.7522},
    {"name": "Helsinki",     "country": "FI", "lat": 60.1699,  "lon": 24.9384},
    {"name": "Warsaw",       "country": "PL", "lat": 52.2297,  "lon": 21.0122},
    {"name": "Prague",       "country": "CZ", "lat": 50.0755,  "lon": 14.4378},
    {"name": "Lisbon",       "country": "PT", "lat": 38.7223,  "lon": -9.1393},
    {"name": "Athens",       "country": "GR", "lat": 37.9838,  "lon": 23.7275},
    {"name": "Cape Town",    "country": "ZA", "lat": -33.9249, "lon": 18.4241},
    {"name": "Lagos",        "country": "NG", "lat": 6.5244,   "lon": 3.3792},
]

BASE_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "wind_speed_10m", "wind_direction_10m", "surface_pressure",
    "cloud_cover", "visibility", "uv_index", "apparent_temperature",
]

def fetch_city_weather(city: dict, days: int = 7) -> pd.DataFrame:
    params = {
        "latitude": city["lat"], "longitude": city["lon"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": days, "timezone": "auto",
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        hourly = data.get("hourly", {})
        if not hourly:
            return pd.DataFrame()
        df = pd.DataFrame(hourly)
        df["city"] = city["name"]
        df["country"] = city["country"]
        df["latitude"] = city["lat"]
        df["longitude"] = city["lon"]
        df["extracted_at"] = datetime.utcnow()
        logger.info(f"Extracted {len(df)} rows for {city['name']}")
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {city['name']}: {e}")
        return pd.DataFrame()

def extract_all_cities(cities=None, days=7, delay=0.2) -> pd.DataFrame:
    cities = cities or CITIES
    all_data = []
    for i, city in enumerate(cities, 1):
        df = fetch_city_weather(city, days=days)
        if not df.empty:
            all_data.append(df)
        if i < len(cities):
            time.sleep(delay)


cd ~/etl_pipeline

# Create etl/__init__.py
cat > etl/__init__.py << 'EOF'
# ETL package
EOF

# Create etl/extract.py
cat > etl/extract.py << 'PYEOF'
"""
extract.py - Data extraction from public APIs
Fetches weather data from Open-Meteo API for multiple cities worldwide.
"""

import requests
import pandas as pd
from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)

CITIES = [
    {"name": "New York",     "country": "US", "lat": 40.7128,  "lon": -74.0060},
    {"name": "London",       "country": "GB", "lat": 51.5074,  "lon": -0.1278},
    {"name": "Tokyo",        "country": "JP", "lat": 35.6762,  "lon": 139.6503},
    {"name": "Paris",        "country": "FR", "lat": 48.8566,  "lon": 2.3522},
    {"name": "Sydney",       "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"name": "Mumbai",       "country": "IN", "lat": 19.0760,  "lon": 72.8777},
    {"name": "Bangalore",    "country": "IN", "lat": 12.9716,  "lon": 77.5946},
    {"name": "Delhi",        "country": "IN", "lat": 28.6139,  "lon": 77.2090},
    {"name": "Chicago",      "country": "US", "lat": 41.8781,  "lon": -87.6298},
    {"name": "Los Angeles",  "country": "US", "lat": 34.0522,  "lon": -118.2437},
    {"name": "Toronto",      "country": "CA", "lat": 43.6532,  "lon": -79.3832},
    {"name": "Berlin",       "country": "DE", "lat": 52.5200,  "lon": 13.4050},
    {"name": "Singapore",    "country": "SG", "lat": 1.3521,   "lon": 103.8198},
    {"name": "Dubai",        "country": "AE", "lat": 25.2048,  "lon": 55.2708},
    {"name": "Sao Paulo",    "country": "BR", "lat": -23.5505, "lon": -46.6333},
    {"name": "Mexico City",  "country": "MX", "lat": 19.4326,  "lon": -99.1332},
    {"name": "Cairo",        "country": "EG", "lat": 30.0444,  "lon": 31.2357},
    {"name": "Istanbul",     "country": "TR", "lat": 41.0082,  "lon": 28.9784},
    {"name": "Moscow",       "country": "RU", "lat": 55.7558,  "lon": 37.6176},
    {"name": "Beijing",      "country": "CN", "lat": 39.9042,  "lon": 116.4074},
    {"name": "Shanghai",     "country": "CN", "lat": 31.2304,  "lon": 121.4737},
    {"name": "Seoul",        "country": "KR", "lat": 37.5665,  "lon": 126.9780},
    {"name": "Bangkok",      "country": "TH", "lat": 13.7563,  "lon": 100.5018},
    {"name": "Lagos",        "country": "NG", "lat": 6.5244,   "lon": 3.3792},
    {"name": "Nairobi",      "country": "KE", "lat": -1.2921,  "lon": 36.8219},
    {"name": "Johannesburg", "country": "ZA", "lat": -26.2041, "lon": 28.0473},
    {"name": "Buenos Aires", "country": "AR", "lat": -34.6037, "lon": -58.3816},
    {"name": "Lima",         "country": "PE", "lat": -12.0464, "lon": -77.0428},
    {"name": "Bogota",       "country": "CO", "lat": 4.7110,   "lon": -74.0721},
    {"name": "Karachi",      "country": "PK", "lat": 24.8607,  "lon": 67.0011},
    {"name": "Dhaka",        "country": "BD", "lat": 23.8103,  "lon": 90.4125},
    {"name": "Jakarta",      "country": "ID", "lat": -6.2088,  "lon": 106.8456},
    {"name": "Manila",       "country": "PH", "lat": 14.5995,  "lon": 120.9842},
    {"name": "Osaka",        "country": "JP", "lat": 34.6937,  "lon": 135.5023},
    {"name": "Houston",      "country": "US", "lat": 29.7604,  "lon": -95.3698},
    {"name": "Phoenix",      "country": "US", "lat": 33.4484,  "lon": -112.0740},
    {"name": "Amsterdam",    "country": "NL", "lat": 52.3676,  "lon": 4.9041},
    {"name": "Madrid",       "country": "ES", "lat": 40.4168,  "lon": -3.7038},
    {"name": "Rome",         "country": "IT", "lat": 41.9028,  "lon": 12.4964},
    {"name": "Vienna",       "country": "AT", "lat": 48.2082,  "lon": 16.3738},
    {"name": "Zurich",       "country": "CH", "lat": 47.3769,  "lon": 8.5417},
    {"name": "Stockholm",    "country": "SE", "lat": 59.3293,  "lon": 18.0686},
    {"name": "Oslo",         "country": "NO", "lat": 59.9139,  "lon": 10.7522},
    {"name": "Helsinki",     "country": "FI", "lat": 60.1699,  "lon": 24.9384},
    {"name": "Warsaw",       "country": "PL", "lat": 52.2297,  "lon": 21.0122},
    {"name": "Prague",       "country": "CZ", "lat": 50.0755,  "lon": 14.4378},
    {"name": "Lisbon",       "country": "PT", "lat": 38.7223,  "lon": -9.1393},
    {"name": "Athens",       "country": "GR", "lat": 37.9838,  "lon": 23.7275},
    {"name": "Cape Town",    "country": "ZA", "lat": -33.9249, "lon": 18.4241},
    {"name": "Lagos",        "country": "NG", "lat": 6.5244,   "lon": 3.3792},
]

BASE_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "wind_speed_10m", "wind_direction_10m", "surface_pressure",
    "cloud_cover", "visibility", "uv_index", "apparent_temperature",
]

def fetch_city_weather(city: dict, days: int = 7) -> pd.DataFrame:
    params = {
        "latitude": city["lat"], "longitude": city["lon"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": days, "timezone": "auto",
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        hourly = data.get("hourly", {})
        if not hourly:
            return pd.DataFrame()
        df = pd.DataFrame(hourly)
        df["city"] = city["name"]
        df["country"] = city["country"]
        df["latitude"] = city["lat"]
        df["longitude"] = city["lon"]
        df["extracted_at"] = datetime.utcnow()
        logger.info(f"Extracted {len(df)} rows for {city['name']}")
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {city['name']}: {e}")
        return pd.DataFrame()

def extract_all_cities(cities=None, days=7, delay=0.2) -> pd.DataFrame:
    cities = cities or CITIES
    all_data = []
    for i, city in enumerate(cities, 1):
        df = fetch_city_weather(city, days=days)
        if not df.empty:
            all_data.append(df)
        if i < len(cities):
            time.sleep(delay)
    if not all_data:
        raise RuntimeError("No data extracted from any city!")
    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Extraction complete: {len(combined):,} total records")
    return combined
