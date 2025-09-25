from flask import Flask, request, jsonify
import requests
from datetime import datetime, timedelta
import pandas as pd

import re
from urllib.parse import urljoin
from urllib.parse import urlencode

import json

app = Flask(__name__)

# THIS IS MY FUCKING API KEY, DO NOT STEAL IT
OGD_API_KEY = "579b464db66ec23bdd000001d4f16af7ec7a4885484833345368a2e7"
OGD_API_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"

@app.route("/marketInfo/prices", methods=["GET"])
def prices():
    # Required filters
    state = request.args.get("state")
    district = request.args.get("district")

    if not state or not district:
        return jsonify({"error": "Missing required filters 'state' and 'district'"}), 400

    # Optional filters
    market = request.args.get("market")
    commodity = request.args.get("commodity")
    variety = request.args.get("variety")
    limit = request.args.get("limit", 100)

    params = {
        "api-key": OGD_API_KEY,
        "format": "json",
        "limit": limit,
    }

    # Add filters with correct OGD syntax
    params["filters[state]"] = state
    params["filters[district]"] = district
    if market:
        params["filters[market]"] = market
    if commodity:
        params["filters[commodity]"] = commodity
    if variety:
        params["filters[variety]"] = variety

    try:
        res = requests.get(OGD_API_URL, params=params, timeout=20)
        res.raise_for_status()
    except Exception as e:
        return jsonify({"error": "Failed to fetch data from OGD API", "details": str(e)}), 502
    
    def convert_quintal_to_kg(price_str):
        try:
            val = float(price_str)
            return round(val / 100.0, 2)  # ₹/quintal to ₹/kg
        except:
            return None

    data = res.json()
    results = []

    fetched_at = datetime.utcnow().isoformat() + "Z"
    for record in data.get("records", []):
        results.append({
            "state": record.get("state"),
            "district": record.get("district"),
            "market": record.get("market"),
            "commodity": record.get("commodity"),
            "variety": record.get("variety"),
            "grade": record.get("grade"),
            "arrival_date": record.get("arrival_date"),
            "price_min_inr_per_kg": convert_quintal_to_kg(record.get("min_price")),
            "price_modal_inr_per_kg": convert_quintal_to_kg(record.get("modal_price")),
            "price_max_inr_per_kg": convert_quintal_to_kg(record.get("max_price")),
            "source": {
                "dataset": "OGD Agmarknet Daily Mandi Prices",
                "resource_id": "9ef84268-d588-465a-a308-a864a43d0070",
                "fetched_at": fetched_at
            }
        })

    return jsonify({
        "count": len(results),
        "results": results
    })

#///////////////////////////////WEATHER//////////////////////////////////////

@app.route("/v1/weather", methods=["GET"])
def weather():
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    # Optional date range or default to today + 7 days forecast
    start_date = request.args.get("start_date", datetime.utcnow().date().isoformat())
    end_date = request.args.get("end_date", (datetime.utcnow() + timedelta(days=7)).date().isoformat())

    def get_open_meteo_forecast(lat, lon, start_date, end_date):
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,precipitation,windspeed_10m,shortwave_radiation",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,et0_fao_evapotranspiration,uv_index_max,uv_index_clear_sky_max",
            "timezone": "Asia/Kolkata"
        }
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()
        return resp.json()


    def get_nasa_power(lat, lon, start_date, end_date):
        base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
        params = {
            "parameters": "ALLSKY_SFC_SW_DWN,T2M,PRECTOT,WS10M",
            "community": "ag",
            "longitude": lon,
            "latitude": lat,
            "start": start_date.replace("-", ""),
            "end": end_date.replace("-", ""),
            "format": "JSON"
        }
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_nasa_power_climatology(lat, lon):
        base_url = "https://power.larc.nasa.gov/api/temporal/climatology/point"
        params = {
            "parameters": "T2M,PRECTOT,ALLSKY_SFC_SW_DWN",
            "community": "ag",
            "longitude": lon,
            "latitude": lat,
            "format": "JSON"
        }
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()
        return resp.json()


    if not lat or not lon:
        return jsonify({"error": "Missing required parameters 'lat' and 'lon'"}), 400

    try:
        open_meteo = get_open_meteo_forecast(lat, lon, start_date, end_date)
        nasa_power = get_nasa_power(lat, lon, start_date, end_date)
        nasa_power_clim = get_nasa_power_climatology(lat, lon)
        # IMD warnings and hydrology data would be additional calls here
    except Exception as e:
        return jsonify({"error": f"Failed to fetch external data", "details": str(e)}), 502

    return jsonify({
        "open_meteo": open_meteo,
        "nasa_power": nasa_power,
        "nasa_power_climatology": nasa_power_clim,
        # Add IMD, WRIS etc when implemented
    })

#############################PESTICIDES###############################
@app.route("/v1/pesticides", methods=["GET"])
def pesticide_check():
    banned_df = pd.read_csv("list_of_banned_pesticides.csv")
    banned_set = set(banned_df['Pesticide_Name'].str.lower().str.strip())
    name = request.args.get("name", "").strip().lower()

    if not name:
        return jsonify({"error": "Please provide pesticide name via 'name' query parameter"}), 400

    status = "banned" if name in banned_set else "safe"

    return jsonify({
        "pesticide": name,
        "status": status
    })


_SOILGRIDS_SCALE = {
    "bdod": 0.01,   # kg/dm3 from cg/cm3 integers
    "cec": 0.1,     # cmol(c)/kg from mmol(c)/kg integers
    "cfvo": 0.1,    # vol%
    "clay": 0.1,    # %
    "nitrogen": 0.01,# g/kg
    "phh2o": 0.1,   # pH
    "sand": 0.1,    # %
    "silt": 0.1,    # %
    "soc": 0.1,     # g/kg
    "ocd": 0.1,     # kg/dm3 (if requested)
    "ocs": 0.1,     # kg/m2 (if requested)
    "wv0010": 0.1,  # vol% (if requested)
    "wv0033": 0.1,  # vol% (if requested)
    "wv1500": 0.1   # vol% (if requested)
}

_SOILGRIDS_PROPS = {
    "phh2o": "soil_pH_H2O",
    "soc": "soil_organic_carbon",
    "clay": "clay_percent",
    "sand": "sand_percent",
    "silt": "silt_percent",
    "bdod": "bulk_density",
    "cec": "cation_exchange_capacity"
}

_STD_DEPTHS = ["0-5", "5-15", "15-30", "30-60", "60-100", "100-200"]

def _norm_depths(depths_in):
    out = []
    for d in depths_in:
        dd = d.strip().lower().replace("cm", "")
        if dd in _STD_DEPTHS:
            out.append(f"{dd}cm")
    return out

def _values_to_dict(vals):
    # SoilGrids v2 may return a dict {"mean":int,"Q0.5":int,...} or a list [{"name":"mean","value":int},...]
    if isinstance(vals, dict):
        return vals
    if isinstance(vals, list):
        bag = {}
        for it in vals:
            nm = (it.get("name") or "").strip()
            if nm:
                bag[nm] = it.get("value")
        return bag
    return {}

@app.route("/v1/soil/soilgrids", methods=["GET"])
def soil_soilgrids():
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    if not lat or not lon:
        return jsonify({"error": "Missing lat/lon"}), 400

    props_q = request.args.get("properties", ",".join(_SOILGRIDS_PROPS.keys())).lower()
    req_props = [p.strip() for p in props_q.split(",") if p.strip()]
    props = [p for p in req_props if p in _SOILGRIDS_PROPS]
    if not props:
        props = list(_SOILGRIDS_PROPS.keys())

    depths_q = request.args.get("depths")
    if depths_q:
        depths = _norm_depths([d for d in depths_q.split(",") if d.strip()])
        if not depths:
            depths = [f"{d}cm" for d in _STD_DEPTHS]
    else:
        depths = [f"{d}cm" for d in _STD_DEPTHS]

    base = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    params = [("lon", lon), ("lat", lat)]
    for p in props:
        params.append(("property", p))
    for d in depths:
        params.append(("depth", d))
    # ask server for all stats; we’ll pick best available below
    try:
        r = requests.get(base, params=params, timeout=25, headers={"User-Agent": "kerala-farm-assist/1.0", "Accept": "application/json"})
        r.raise_for_status()
        js = r.json()
    except Exception as e:
        return jsonify({"error": "Failed to fetch SoilGrids", "details": str(e)}), 502

    layers = None
    if isinstance(js, dict):
        layers = js.get("properties", {}).get("layers") or js.get("layers")
    if not isinstance(layers, list):
        return jsonify({"error": "Unexpected SoilGrids schema", "raw_keys": list(js.keys()) if isinstance(js, dict) else "n/a"}), 502

    results = []
    for layer in layers:
        name = layer.get("name")
        scale = _SOILGRIDS_SCALE.get(name, 1.0)
        friendly = _SOILGRIDS_PROPS.get(name, name)
        for dp in layer.get("depths", []):
            label = dp.get("label")
            vals_raw = dp.get("values")
            vals = _values_to_dict(vals_raw)
            # prefer mean, fallback to Q0.5, final fallback to midpoint of Q0.05/Q0.95
            val = vals.get("mean")
            if val is None:
                val = vals.get("Q0.5") or vals.get("Q50") or vals.get("median")
            if val is None and vals.get("Q0.05") is not None and vals.get("Q0.95") is not None:
                try:
                    val = (float(vals.get("Q0.05")) + float(vals.get("Q0.95"))) / 2.0
                except:
                    val = None
            if val is not None:
                try:
                    val = float(val) * scale
                except:
                    val = None
            results.append({
                "property": friendly,
                "depth": label,
                "value_type": "mean_or_median_scaled",
                "value": val
            })

    return jsonify({
        "query": {"lat": lat, "lon": lon, "properties": props, "depths": depths, "stat_preference": ["mean", "Q0.5", "mid(Q0.05,Q0.95)"]},
        "count": len(results),
        "results": results,
        "source": {"service": "ISRIC SoilGrids v2", "endpoint": base}
    })
# ==========================================================
# AIR QUALITY: OpenAQ v2 — latest measurements near a point
# ==========================================================

@app.route("/v1/air/nearest", methods=["GET"])
def air_nearest():
    """
    Latest air quality measurements from OpenAQ near a coordinate.
    Params:
      - lat (required)
      - lon (required)
      - radius_m (optional, default 10000)
      - limit (optional, default 5)
      - parameters (optional, comma-separated e.g., pm25,pm10,no2,o3,so2,co)
    """
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    if not lat or not lon:
        return jsonify({"error": "Missing lat/lon"}), 400

    try:
        radius = int(request.args.get("radius_m", 10000))
    except:
        radius = 10000

    try:
        limit = int(request.args.get("limit", 5))
    except:
        limit = 5

    params = {
        "coordinates": f"{lat},{lon}",
        "radius": radius,
        "limit": limit,
        "order_by": "distance",
        "sort": "asc"
    }
    # Optional filter by pollutant parameters
    parameters_csv = request.args.get("parameters")
    if parameters_csv:
        params["parameters[]"] = [p.strip() for p in parameters_csv.split(",") if p.strip()]

    url = "https://api.openaq.org/v2/latest"
    try:
        r = requests.get(url, params=params, timeout=20, headers={"User-Agent": "kerala-farm-assist/1.0"})
        r.raise_for_status()
        js = r.json()
    except Exception as e:
        return jsonify({"error": "Failed to fetch OpenAQ", "details": str(e)}), 502

    # Simplify output
    simplified = []
    for loc in js.get("results", []):
        coord = loc.get("coordinates") or {}
        for m in loc.get("measurements", []):
            simplified.append({
                "location": loc.get("location"),
                "distance_m": loc.get("distance"),
                "parameter": m.get("parameter"),
                "value": m.get("value"),
                "unit": m.get("unit"),
                "last_updated": m.get("lastUpdated"),
                "lat": coord.get("latitude"),
                "lon": coord.get("longitude"),
                "city": loc.get("city"),
                "country": loc.get("country")
            })

    return jsonify({
        "query": {"lat": lat, "lon": lon, "radius_m": radius, "limit": limit, "parameters": params.get("parameters[]")},
        "count": len(simplified),
        "results": simplified,
        "source": {"service": "OpenAQ v2", "endpoint": url}
    })

# =================================================
# GEOCODING: Nominatim (OpenStreetMap) — no API key
# =================================================

_NOMINATIM_BASE = "https://nominatim.openstreetmap.org"

def _nominatim_headers():
    # Respect usage policy: set a descriptive User-Agent
    return {"User-Agent": "kerala-farm-assist/1.0"}

@app.route("/v1/geocode/search", methods=["GET"])
def geocode_search():
    """
    Forward geocoding with Nominatim.
    Params:
      - q (required): free text, e.g., "Aluva market Ernakulam Kerala"
      - limit (optional, default 5)
      - countrycodes (optional, default 'in')
    """
    q = request.args.get("q")
    if not q:
        return jsonify({"error": "Missing 'q'"}), 400

    try:
        limit = int(request.args.get("limit", 5))
    except:
        limit = 5

    countrycodes = request.args.get("countrycodes", "in")

    params = {
        "q": q,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": limit,
        "countrycodes": countrycodes
    }
    url = f"{_NOMINATIM_BASE}/search"
    try:
        r = requests.get(url, params=params, timeout=20, headers=_nominatim_headers())
        r.raise_for_status()
        items = r.json()
    except Exception as e:
        return jsonify({"error": "Failed to geocode", "details": str(e)}), 502

    results = []
    for it in items:
        results.append({
            "display_name": it.get("display_name"),
            "lat": float(it.get("lat")) if it.get("lat") else None,
            "lon": float(it.get("lon")) if it.get("lon") else None,
            "type": it.get("type"),
            "class": it.get("class"),
            "importance": it.get("importance"),
            "address": it.get("address")
        })

    return jsonify({
        "count": len(results),
        "results": results,
        "source": {"service": "Nominatim", "endpoint": url}
    })

@app.route("/v1/geocode/reverse", methods=["GET"])
def geocode_reverse():
    """
    Reverse geocoding with Nominatim.
    Params:
      - lat (required)
      - lon (required)
      - zoom (optional, 0..18, default 16)
    """
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    if not lat or not lon:
        return jsonify({"error": "Missing lat/lon"}), 400

    try:
        zoom = int(request.args.get("zoom", 16))
    except:
        zoom = 16

    params = {
        "lat": lat,
        "lon": lon,
        "format": "jsonv2",
        "addressdetails": 1,
        "zoom": zoom
    }
    url = f"{_NOMINATIM_BASE}/reverse"
    try:
        r = requests.get(url, params=params, timeout=20, headers=_nominatim_headers())
        r.raise_for_status()
        js = r.json()
    except Exception as e:
        return jsonify({"error": "Failed to reverse geocode", "details": str(e)}), 502

    out = {
        "display_name": js.get("display_name"),
        "lat": float(js.get("lat")) if js.get("lat") else None,
        "lon": float(js.get("lon")) if js.get("lon") else None,
        "address": js.get("address")
    }
    return jsonify({
        "result": out,
        "source": {"service": "Nominatim", "endpoint": url}
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
