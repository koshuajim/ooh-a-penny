import requests
from datetime import datetime
import numpy as np
import pandas as pd
import json
import argparse
from pathlib import Path

DATA_FILE = Path("data_log.json")
BASE = "https://api.elections.kalshi.com/trade-api/v2"
high_series = {
    "la": "KXHIGHLAX",
    "den": "KXHIGHDEN",
    "ny": "KXHIGHNY",
    "chi": "KXHIGHCHI",
    "mia": "KXHIGHMIA",
    "aus": "KXHIGHAUS",
    "phil": "KXHIGHPHIL",
}

low_series = {
    "la": "KXLOWTLAX",
    "den": "KXLOWTDEN",
    "ny": "KXLOWTNY",
    "chi": "KXLOWTCHI",
    "mia": "KXLOWTMIA",
    "aus": "KXLOWTAUS",
    "phil": "KXLOWTPHIL",
}

meteo_ensembles = {
    "la": "ecmwf_ifs025",
    "den": "gfs_seamless",
    "ny": "ecmwf_ifs025",
    "chi": "ecmwf_ifs025",
    "mia": "ecmwf_ifs025",
    "aus": "gfs_seamless",
    "phil": "ecmwf_ifs025",
}

city_coords = {
    "mia": (25.78805, -80.31694),
    "la": (33.93816, -118.38660),
    "ny": (40.77898, -73.96925),
    "aus": (30.18304, -97.67987),
    "chi": (41.78412, -87.75514),
    "phil": (39.87326, -75.22681),
    "den": (39.76746, -104.86948)
}

def grab_high_single(city_code):
    coords = city_coords[city_code]
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords[0],
        "longitude": coords[1],
        "daily": "temperature_2m_max",
        "timezone": "auto",
        "forecast_days": 2,
        "temperature_unit": "fahrenheit"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    max_temp_today = data["daily"]["temperature_2m_max"][0]
    max_temp_tmrw = data["daily"]["temperature_2m_max"][1]
    
    return max_temp_today, max_temp_tmrw

def grab_low_single(city_code):
    coords = city_coords[city_code]
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords[0],
        "longitude": coords[1],
        "daily": "temperature_2m_min",
        "timezone": "auto",
        "forecast_days": 2,
        "temperature_unit": "fahrenheit"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    min_temp_today = data["daily"]["temperature_2m_min"][0]
    min_temp_tmrw = data["daily"]["temperature_2m_min"][1]
    
    return min_temp_today, min_temp_tmrw

def grab_high_ensemble(city_code):
    coords = city_coords[city_code]
    model = meteo_ensembles[city_code]
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": coords[0],
        "longitude": coords[1],
        "daily": "temperature_2m_max",
        "models": model,
        "timezone": "auto",
        "forecast_days": 2,
        "temperature_unit": "fahrenheit"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    today_highs = []
    tmrw_highs = []
    
    for key in data["daily"]:
        if "temperature" in key:
            temps = data["daily"][key]
            today_highs.append(temps[0])
            tmrw_highs.append(temps[1])
    
    today_highs = np.array(today_highs)
    tmrw_highs = np.array(tmrw_highs)
    
    return today_highs, tmrw_highs

def grab_low_ensemble(city_code):
    coords = city_coords[city_code]
    model = meteo_ensembles[city_code]
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": coords[0],
        "longitude": coords[1],
        "daily": "temperature_2m_min",
        "models": model,
        "timezone": "auto",
        "forecast_days": 2,
        "temperature_unit": "fahrenheit"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    today_lows = []
    tmrw_lows = []
    
    for key in data["daily"]:
        if "temperature" in key:
            temps = data["daily"][key]
            today_lows.append(temps[0])
            tmrw_lows.append(temps[1])
    
    return today_lows, tmrw_lows

def grab_available_events(city_code, today=True, high=True):
    if high:
        r = requests.get(
            f"{BASE}/markets",
            params = {"series_ticker": high_series[city_code], "status": "open"}
        )
    else:
        r = requests.get(
            f"{BASE}/markets",
            params = {"series_ticker": low_series[city_code], "status": "open"}
        )
    
    markets = r.json()["markets"]
    available_events = []
    for market in markets:
        if market['status'] == 'active':
            available_events.append(market['ticker'])
    
    dateString = datetime.now().strftime("%y%b%d").upper() if today else (datetime.now() + pd.Timedelta(days=1)).strftime("%y%b%d").upper()
    
    filtered_events = [event for event in available_events if dateString in event]
    return filtered_events

def grab_prices(city_code, today=True, high=True):
    event_tickers = grab_available_events(city_code, today=today, high=high)
    
    prices = {}
    
    for ticker in event_tickers:
        r = requests.get(
            f"{BASE}/markets/{ticker}/orderbook",
        )
        
        orderbook = r.json()["orderbook"]
        price = 100 - max(row[0] for row in orderbook["no"])
        prices[ticker] = price
    
    return prices
    
def log_data_point(city_code, today=True, dry_run=False):
    high_single_today, high_single_tmrw = grab_high_single(city_code)
    high_ensemble_today, high_ensemble_tmrw = grab_high_ensemble(city_code)
    
    low_single_today, low_single_tmrw = grab_low_single(city_code)
    low_ensemble_today, low_ensemble_tmrw = grab_low_ensemble(city_code)
    
    high_prices = grab_prices(city_code, today=today, high=True)
    low_prices = grab_prices(city_code, today=today, high=False)
    
    data_point = {
        "city": city_code,
        "timestamp": datetime.now().isoformat(),
        "high_single": high_single_today if today else high_single_tmrw,
        "high_ensemble": high_ensemble_today if today else high_ensemble_tmrw,
        "high_prices": high_prices,
        "low_single": low_single_today if today else low_single_tmrw,
        "low_ensemble": low_ensemble_today if today else low_ensemble_tmrw,
        "low_prices": low_prices
    }
    
    if not dry_run:
        if DATA_FILE.exists():
            data = json.loads(DATA_FILE.read_text())
        else:
            data = []
        
        data.append(data_point)
        DATA_FILE.write_text(json.dumps(data, indent=2))
    
    print("Logged Data Point for ", city_code, " today=", today, " dry_run=", dry_run)
    
    
    

# la 12 am same, 4 pm same
# denver 11 pm previous, 3 pm same
# nyc 9 pm previous, 1 pm same
# chi 10 pm previous, 2 pm same
# mia 9 pm previous, 1 pm same
# aus 10 pm previous, 2 pm same
# phil 9 pm previous, 1 pm same

# hours to collect locally
# 9 p, 10 p, 11 p, 12 s, 1 s, 2 s, 3 s, 4 s

# print(balance)
# grab_high_orderbook_tdy("la")

# log_data_point("la", today=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, required=True)
    parser.add_argument("--today", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    
    args = parser.parse_args()
    
    log_data_point(
        city_code=args.city,
        today=args.today,
        dry_run=args.dry_run
    )