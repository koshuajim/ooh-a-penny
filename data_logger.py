import requests
from datetime import datetime
import numpy as np
import pandas as pd
import json
from pathlib import Path
from zoneinfo import ZoneInfo

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
    
    response = requests.get(url, params=params, timeout=10)
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
    
    response = requests.get(url, params=params, timeout=10)
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
    
    response = requests.get(url, params=params, timeout=10)
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
    
    response = requests.get(url, params=params, timeout=10)
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
            , timeout=10
        )
    else:
        r = requests.get(
            f"{BASE}/markets",
            params = {"series_ticker": low_series[city_code], "status": "open"}
            , timeout=10
        )

    print(r.json())
    markets = r.json()["market"]
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
            timeout=10
        )
        
        orderbook = r.json()["orderbook"]
        no_orders = orderbook.get("no")

        if not no_orders or any(row is None or row[0] is None for row in no_orders):
            price = -1
        else:
            price = 100 - max(row[0] for row in no_orders)
            
        prices[ticker] = price
    
    return prices
    
def log_data_point(city, today=True, dry_run=False):
    high_single_today, high_single_tmrw = grab_high_single(city)
    high_ensemble_today, high_ensemble_tmrw = grab_high_ensemble(city)
    
    low_single_today, low_single_tmrw = grab_low_single(city)
    low_ensemble_today, low_ensemble_tmrw = grab_low_ensemble(city)
    
    high_prices = grab_prices(city, today=today, high=True)
    low_prices = grab_prices(city, today=today, high=False)
    
    data_point = {
        "city": city,
        "timestamp": datetime.now(ZoneInfo("America/Los_Angeles")).isoformat(),
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
    
    print("Logged Data Point for ", city, " today=", today, " dry_run=", dry_run)
    

PARAMS_BY_HOUR = {
    20: [{"city": "ny", "today": False}, {"city": "mia", "today": False}, {"city": "phil", "today": False}],
    21: [{"city": "ny", "today": False}, {"city": "mia", "today": False}, {"city": "phil", "today": False}],
    22: [{"city": "ny", "today": False}, {"city": "mia", "today": False}, {"city": "phil", "today": False}, {"city": "chi", "today": False}, {"city": "aus", "today": False}],
    23: [{"city": "ny", "today": False}, {"city": "mia", "today": False}, {"city": "phil", "today": False}, {"city": "chi", "today": False}, {"city": "aus", "today": False}, {"city": "den", "today": False}],
    0:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    1:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    2:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    3:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    4:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    5:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    6:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    7:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    8:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    9:  [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    10: [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    11: [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    12: [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    13: [{"city": "ny", "today": True}, {"city": "mia", "today": True}, {"city": "phil", "today": True}, {"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    14: [{"city": "chi", "today": True}, {"city": "aus", "today": True}, {"city": "den", "today": True}, {"city": "la", "today": True}],
    15: [{"city": "den", "today": True}, {"city": "la", "today": True}],
    16: [{"city": "la", "today": True}],
}

# la 12 am same, 4 pm same
# denver 11 pm previous, 3 pm same
# nyc 9 pm previous, 1 pm same
# chi 10 pm previous, 2 pm same
# mia 9 pm previous, 1 pm same
# aus 10 pm previous, 2 pm same
# phil 9 pm previous, 1 pm same

# 9pm, 10pm, 11pm, 12am, 1 am, 2 am, 3 am, 4 am, 5 am, 6 am, 7 am, 8 am, 9 am, 10 am, 11 am, 12 pm, 1 pm, 2 pm, 3 pm, 4pm

# print(balance)
# grab_high_orderbook_tdy("la")

# log_data_point("la", today=False)

if __name__ == "__main__":
    now_la = datetime.now(ZoneInfo("America/Los_Angeles"))
    hour = now_la.hour
    
    params = PARAMS_BY_HOUR.get(hour)
    
    if not params:
        print("No data points scheduled for this hour.")
        raise SystemExit(0)
    
    for p in params:

        log_data_point(**p)


