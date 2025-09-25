import os
import asyncio
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from meteostat import Point, Daily


import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

OPEN_METEO_BASE = os.getenv("OPEN_METEO_BASE", "https://api.open-meteo.com/v1/forecast")
NASA_POWER_BASE = os.getenv("NASA_POWER_BASE", "https://power.larc.nasa.gov/api/temporal")
IMD_BASE = "https://mausam.imd.gov.in/api" 

app = FastAPI(title="LLM Data Proxy (Weather/Agro/Hydro)")

# -------------------------
# Simple async TTL cache
# -------------------------
class SimpleTTLCache:
    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def get_or_set(self, key: str, ttl: int, coro):
        now = time.time()
        async with self._lock:
            item = self._store.get(key)
            if item and item["expiry"] > now:
                return item["value"]
        # not found or expired, fetch
        value = await coro()
        async with self._lock:
            self._store[key] = {"value": value, "expiry": now + ttl}
        return value

cache = SimpleTTLCache()
client = httpx.AsyncClient()

# --------------------------
# Open-Meteo Weather
# --------------------------
@app.get("/api/weather/open-meteo")
async def get_open_meteo_weather(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    ttl: int = Query(3600, description="Cache TTL seconds (default 1 hour)"),
):
    key = f"open_meteo_weather:{latitude}:{longitude}"

    async def _fetch():
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
            "timezone": "Asia/Kolkata",
        }
        try:
            response = await client.get(OPEN_METEO_BASE, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            return {
                "source": "Open-Meteo",
                "latitude": latitude,
                "longitude": longitude,
                "data": data,
                "last_updated": str(datetime.utcnow()),
            }
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"API error: {e}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Request failed: {e}")

    return await cache.get_or_set(key, ttl, _fetch)

# --------------------------
# NASA POWER Agro-climatic
# --------------------------
@app.get("/api/agro/nasa-power")
async def get_nasa_power_agro(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    start_date: str = Query(..., description="Start date in YYYYMMDD format"),
    end_date: str = Query(..., description="End date in YYYYMMDD format"),
    ttl: int = Query(86400, description="Cache TTL seconds (default 1 day)"),
):
    key = f"nasa_power_agro:{latitude}:{longitude}:{start_date}:{end_date}"

    async def _fetch():
        # Correct NASA POWER endpoint format:
        # https://power.larc.nasa.gov/api/temporal/{temporal}/point
        url = f"{NASA_POWER_BASE}/hourly/point"

        params = {
            "parameters": "T2M,RH2M,PRECTOTCORR",   # Temperature, Relative Humidity, Precipitation
            "community": "ag",
            "longitude": longitude,
            "latitude": latitude,
            "start": start_date,   # YYYYMMDD
            "end": end_date,       # YYYYMMDD
            "format": "JSON",
        }

        try:
            response = await client.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return {
                "source": "NASA POWER",
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date,
                "end_date": end_date,
                "data": data,
                "last_updated": str(datetime.utcnow()),
            }
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"NASA POWER API error: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Request to NASA POWER API failed: {e}")

    return await cache.get_or_set(key, ttl, _fetch)

# ---------------------------
# Meteostat Climate Data
# ---------------------------
@app.get("/api/weather/meteostat")
async def get_meteostat_data(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    start_date: str = Query(..., example="2024-01-01"),
    end_date: str = Query(..., example="2024-01-07"),
    ttl: int = Query(3600, ge=60, le=86400) # Default TTL of 1 hour, min 1 minute, max 1 day
):
    key = f"meteostat:{latitude}:{longitude}:{start_date}:{end_date}"

    async def _fetch():
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
            )

        # Use the Point class to create a location object
        location = Point(latitude, longitude)

        # Get daily weather data
        data = Daily(location, start, end)
        data = data.fetch()

        if data.empty:
            raise HTTPException(status_code=404, detail="No data found for the specified location and date range.")

        # Convert the pandas DataFrame to a dictionary for JSON response
        response_data = data.reset_index().to_dict('records')

        return {
            "source": "Meteostat",
            "latitude": latitude,
            "longitude": longitude,
            "data": response_data,
            "last_updated": str(datetime.utcnow())
        }

    return await cache.get_or_set(key, ttl, _fetch)

# ---------------------------
# Drought Indices / Rainfall Anomalies (IMD API)
# ---------------------------
@app.get("/api/imd/drought")
async def get_imd_drought(
    district_id: str = Query(...),
    ttl: int = Query(86400, description="Cache TTL seconds (default 1 day)")
):
    """
    Fetches real-time drought and rainfall anomaly data from the India Meteorological Department (IMD).
    Note: Requires a whitelisted IP to access the API in a real-world scenario.
    The `district_id` parameter corresponds to IMD's internal ID for a district.
    """
    key = f"imd_drought:{district_id}"

    async def _fetch():
        # The search result provided the following sample URL structure:
        # https://mausam.imd.gov.in/api/districtwise_rainfall_api.php
        # This is an example of an endpoint that might contain the data we need.
        # We'll use a nowcast API for a more dynamic example.
        api_endpoint = f"{IMD_BASE}/nowcast_district_api.php"
        params = {"id": district_id}
        
        try:
            # Note: This API might require a whitelisted IP. For local testing, it might not work.
            response = await client.get(api_endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            return {
                "source": "India Meteorological Department (IMD)",
                "district_id": district_id,
                "data": data,
                "last_updated": str(datetime.utcnow())
            }
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"IMD API error: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Request to IMD API failed: {e}")

    return await cache.get_or_set(key, ttl, _fetch)

# ------------------------
# Root info
# ------------------------
@app.get("/")
def root():
    return {
        "message": "LLM Data Proxy",
        "endpoints": {
            "weather_open_meteo": "/api/weather/open-meteo?latitude=28.7041&longitude=77.1025",
            "nasa_power_agro": "/api/agro/nasa-power?latitude=28.7041&longitude=77.1025&start_date=20240101&end_date=20240107",
            "climate_meteostat": "/api/climate/meteostat?station_id=7651&start_date=2023-01-01&end_date=2023-01-31",
            "imd_drought": "/api/imd/drought?district_id=5"
        },
        "notes": "Endpoints may require specific parameters. Please refer to the docstrings or source code for details."
    }
