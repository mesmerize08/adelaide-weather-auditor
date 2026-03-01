import os
import sys
import time
import requests
import re
import pandas as pd
import pytz
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# --- 1. Date Tracking ---
adelaide_tz = pytz.timezone('Australia/Adelaide')
now = datetime.now(adelaide_tz)

today_str = now.strftime('%Y-%m-%d')
yesterday_str = (now - timedelta(days=1)).strftime('%Y-%m-%d')
CSV_FILE = 'weather_history.csv'

STATIONS = {
    'West Terrace': {'lat': -34.9285, 'lon': 138.5955, 'bom_id': 94648},
    'Airport': {'lat': -34.9524, 'lon': 138.5196, 'bom_id': 94672},
    'Kent Town': {'lat': -34.9211, 'lon': 138.6216, 'bom_id': 94643},
    'Mt Lofty': {'lat': -34.9800, 'lon': 138.7083, 'bom_id': 94693}
}

if not os.path.exists(CSV_FILE):
    df = pd.DataFrame(columns=[
        'Date', 'Station', 'Source', 'Forecast_Min_Temp', 'Forecast_Max_Temp', 
        'Forecast_Rain_Prob', 'Forecast_Rain_Min_mm', 'Forecast_Rain_Max_mm', 
        'Actual_Min_Temp', 'Actual_Max_Temp', 'Actual_Rain_mm'
    ])
    df.to_csv(CSV_FILE, index=False)

df_history = pd.read_csv(CSV_FILE)

# Check to prevent duplicate forecast entries if GitHub runs this twice
already_ran_today = today_str in df_history['Date'].values
new_records = []

# --- 2. Fetch Forecasts (Today) ---
def fetch_open_meteo(lat, lon, model):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min,precipitation_probability_max,precipitation_sum&timezone=Australia%2FAdelaide&forecast_days=1&models={model}"
    try:
        res = requests.get(url).json()
        return {
            'Min_Temp': res['daily']['temperature_2m_min'][0],
            'Max_Temp': res['daily']['temperature_2m_max'][0],
            'Rain_Prob': res['daily']['precipitation_probability_max'][0],
            'Rain_Min': res['daily']['precipitation_sum'][0],
            'Rain_Max': res['daily']['precipitation_sum'][0]
        }
    except Exception as e:
        print(f"Open-Meteo ({model}) Fetch Error: {e}")
        return None

def scrape_weatherzone():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    url = "https://www.weatherzone.com.au/sa/adelaide/adelaide"
    try:
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        rain_text = soup.find(string=re.compile(r'\d+.*mm')).strip()
        matches = re.findall(r'\d+', rain_text)
        rain_min = float(matches[0])
        rain_max = float(matches[1]) if len(matches) > 1 else rain_min
        return {
            'Min_Temp': float(soup.find(class_='min-temp').text.strip('°')),
            'Max_Temp': float(soup.find(class_='max-temp').text.strip('°')),
            'Rain_Prob': float(soup.find(class_='rain-prob').text.strip('%')),
            'Rain_Min': rain_min,
            'Rain_Max': rain_max
        }
    except Exception as e:
        print(f"Weatherzone Scrape Error: {e}")
        return None

if not already_ran_today:
    open_meteo_models = {
        'Open-Meteo (ECMWF)': 'ecmwf_ifs04',
        'Open-Meteo (GFS)': 'gfs_seamless',
        'Open-Meteo (BOM)': 'bom_access_global'
    }
    wz_data = scrape_weatherzone()
    
    for name, coords in STATIONS.items():
        for source_name, model_code in open_meteo_models.items():
            om_data = fetch_open_meteo(coords['lat'], coords['lon'], model_code)
            if om_data:
                new_records.append({'Date': today_str, 'Station': name, 'Source': source_name, **om_data})
        if wz_data:
            new_records.append({'Date': today_str, 'Station': name, 'Source': 'Weatherzone', **wz_data})
else:
    print(f"Forecasts for {today_str} already recorded. Checking Actuals only.")

# --- 3. Update Yesterday's Actuals (BOM) ---
def fetch_bom_actuals(bom_id):
    url = f"http://reg.bom.gov.au/fwo/IDS60901/IDS60901.{bom_id}.json"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'http://www.bom.gov.au/'
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        
        if res.status_code != 200:
            print(f"BOM Fetch Error for {bom_id}: HTTP {res.status_code} - Firewall block likely.")
            return None
            
        data = res.json()['observations']['data']
        max_t = max([x['air_temp'] for x in data[:48]])
        min_t = min([x['air_temp'] for x in data[:48]])
        rain = data[0]['rain_trace']
        return {'Actual_Min_Temp': min_t, 'Actual_Max_Temp': max_t, 'Actual_Rain_mm': float(rain) if rain != '-' else 0.0}
    except Exception as e:
        print(f"BOM Fetch Error for {bom_id}: {e}")
        return None

for name, coords in STATIONS.items():
    actuals = fetch_bom_actuals(coords['bom_id'])
    if actuals:
        mask = (df_history['Date'] == yesterday_str) & (df_history['Station'] == name)
        df_history.loc[mask, 'Actual_Min_Temp'] = actuals['Actual_Min_Temp']
        df_history.loc[mask, 'Actual_Max_Temp'] = actuals['Actual_Max_Temp']
        df_history.loc[mask, 'Actual_Rain_mm'] = actuals['Actual_Rain_mm']
        print(f"Successfully fetched BOM actuals for {name}.")
    
    # CRITICAL: 3 second delay to prevent BOM firewall rate-limiting
    time.sleep(3)

# --- 4. Save and Append ---
if not already_ran_today:
    df_new = pd.DataFrame(new_records)
    df_final = pd.concat([df_history, df_new], ignore_index=True)
else:
    df_final = df_history

df_final.to_csv(CSV_FILE, index=False)
print("Pipeline execution complete.")
