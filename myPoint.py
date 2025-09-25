from flask import Flask, request, jsonify
import requests
from datetime import datetime, timedelta
import pandas as pd

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
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
