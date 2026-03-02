import os
import sys
import time
import requests
import re
import pandas as pd
import pytz
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# --- 1. Date & Time Setup ---
adelaide_tz = pytz.timezone('Australia/Adelaide')
now = datetime.now(adelaide_tz)

today_str = now.strftime('%Y-%m-%d')
yesterday_str = (now - timedelta(days=1)).strftime('%Y-%m-%d')
CSV_FILE = 'weather_history.csv'

# --- 9 AM Guard ---
# GitHub Actions cron targets ~9:00 AM Adelaide time (ACST/ACDT).
# Only allow execution between 8:30 AM and 10:00 AM Adelaide time.
current_hour = now.hour
current_minute = now.minute
time_in_minutes = current_hour * 60 + current_minute

if not (510 <= time_in_minutes <= 600):  # 8:30 AM to 10:00 AM
    print(f"Current Adelaide time: {now.strftime('%H:%M')} — outside the 8:30–10:00 AM window. Exiting.")
    sys.exit(0)

print(f"Running at Adelaide time: {now.strftime('%Y-%m-%d %H:%M %Z')}")

# --- Station Configuration ---
# 4 stations across Adelaide metro area.
# Each has:
#   - lat/lon: for Open-Meteo API queries
#   - bom_id/prod_id: for fetching BOM observation actuals (JSON feed)
#   - bom_place: URL slug for scraping the BOM published forecast page
STATIONS = {
    'West Terrace': {
        'lat': -34.9250, 'lon': 138.5870,
        'bom_id': '94648', 'prod_id': 'IDS60901',
        'bom_place': 'adelaide',
    },
    'Adelaide Airport': {
        'lat': -34.9524, 'lon': 138.5196,
        'bom_id': '94146', 'prod_id': 'IDS60901',
        'bom_place': 'adelaide-airport',
    },
    'Mount Lofty': {
        'lat': -34.9800, 'lon': 138.7083,
        'bom_id': '95678', 'prod_id': 'IDS60901',
        'bom_place': 'mount-lofty',
    },
    'Noarlunga': {
        'lat': -35.1667, 'lon': 138.4833,
        'bom_id': '94808', 'prod_id': 'IDS60901',
        'bom_place': 'noarlunga-centre',
    },
}

# --- CSV Initialisation ---
CSV_COLUMNS = [
    'Date', 'Station', 'Source',
    'Forecast_Min_Temp', 'Forecast_Max_Temp',
    'Forecast_Rain_Prob', 'Forecast_Rain_Min_mm', 'Forecast_Rain_Max_mm',
    'Actual_Min_Temp', 'Actual_Max_Temp', 'Actual_Rain_mm'
]

if not os.path.exists(CSV_FILE):
    df = pd.DataFrame(columns=CSV_COLUMNS)
    df.to_csv(CSV_FILE, index=False)

df_history = pd.read_csv(CSV_FILE)

# Duplicate guard: skip forecasts if today's data already exists
already_ran_today = today_str in df_history['Date'].values
new_records = []


# ============================================================
#  2. FORECAST FETCHERS — Three competing sources
# ============================================================

# --- SOURCE 1: BOM Published Forecast ---
def fetch_bom_forecast(bom_place):
    """
    Scrape today's published BOM forecast from bom.gov.au/places/.

    This is the human-curated forecast that BOM meteorologists issue —
    the prediction most Australians actually see and rely on.

    BOM pages use a consistent <dt>/<dd> structure:
        Min: 13 °C  |  Max: 24 °C
        Possible rainfall: 0 to 1 mm
        Chance of any rain: 40%

    We extract the FIRST forecast block (today / rest of today).
    """
    url = f"https://www.bom.gov.au/places/sa/{bom_place}/forecast"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html',
    }
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        min_temp = None
        max_temp = None
        rain_min = 0.0
        rain_max = 0.0
        rain_prob = None

        for dt in soup.find_all('dt'):
            label = dt.get_text(strip=True).lower()
            dd = dt.find_next_sibling('dd')
            if dd is None:
                continue
            value = dd.get_text(strip=True)

            if label == 'min' and min_temp is None:
                match = re.search(r'(-?\d+)', value)
                if match:
                    min_temp = float(match.group(1))

            elif label == 'max' and max_temp is None:
                match = re.search(r'(-?\d+)', value)
                if match:
                    max_temp = float(match.group(1))

            elif 'possible rainfall' in label and rain_min == 0.0 and rain_max == 0.0:
                nums = re.findall(r'(\d+\.?\d*)', value)
                if len(nums) >= 2:
                    rain_min = float(nums[0])
                    rain_max = float(nums[1])
                elif len(nums) == 1:
                    rain_min = float(nums[0])
                    rain_max = float(nums[0])

            elif 'chance of any rain' in label and rain_prob is None:
                match = re.search(r'(\d+)', value)
                if match:
                    rain_prob = float(match.group(1))

            # Stop after we have a complete first-day forecast
            if min_temp is not None and max_temp is not None and rain_prob is not None:
                break

        if max_temp is None:
            print(f"  BOM ({bom_place}): Could not extract max temperature.")
            return None

        print(f"  BOM ({bom_place}): Max={max_temp}, Min={min_temp}, "
              f"Rain={rain_min}-{rain_max}mm, Prob={rain_prob}%")

        return {
            'Forecast_Min_Temp': min_temp,
            'Forecast_Max_Temp': max_temp,
            'Forecast_Rain_Prob': rain_prob,
            'Forecast_Rain_Min_mm': rain_min,
            'Forecast_Rain_Max_mm': rain_max,
        }
    except requests.exceptions.RequestException as e:
        print(f"  BOM ({bom_place}) HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"  BOM ({bom_place}) Parse Error: {e}")
        return None


# --- SOURCE 2: Open-Meteo (Best Match) ---
def fetch_open_meteo(lat, lon):
    """
    Fetch daily forecast from Open-Meteo using "Best Match" mode.

    When no &models= parameter is specified, Open-Meteo automatically
    selects the best available model for the given location. For Adelaide
    this typically blends high-resolution local models with global ones.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,"
        f"precipitation_probability_max,precipitation_sum"
        f"&timezone=Australia%2FAdelaide&forecast_days=1"
    )
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        data = res.json()

        if 'daily' not in data:
            print(f"  Open-Meteo: No 'daily' key in response.")
            return None

        daily = data['daily']
        max_temp = daily.get('temperature_2m_max', [None])[0]
        min_temp = daily.get('temperature_2m_min', [None])[0]
        rain_sum = daily.get('precipitation_sum', [None])[0]
        rain_prob = daily.get('precipitation_probability_max', [None])[0]

        if max_temp is None or min_temp is None:
            print(f"  Open-Meteo: Returned null temperature data.")
            return None

        print(f"  Open-Meteo: Max={max_temp}, Min={min_temp}, "
              f"Rain={rain_sum}mm, Prob={rain_prob}%")

        return {
            'Forecast_Min_Temp': min_temp,
            'Forecast_Max_Temp': max_temp,
            'Forecast_Rain_Prob': rain_prob,
            'Forecast_Rain_Min_mm': rain_sum if rain_sum is not None else 0.0,
            'Forecast_Rain_Max_mm': rain_sum if rain_sum is not None else 0.0,
        }
    except requests.exceptions.RequestException as e:
        print(f"  Open-Meteo HTTP Error: {e}")
        return None
    except (KeyError, IndexError, ValueError) as e:
        print(f"  Open-Meteo Parse Error: {e}")
        return None


# --- SOURCE 3: Weatherzone ---
def scrape_weatherzone():
    """
    Scrape today's Adelaide forecast from Weatherzone.

    WARNING: Web scraping is fragile. If Weatherzone changes their
    HTML structure, this will need selector updates.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    url = "https://www.weatherzone.com.au/sa/adelaide/adelaide"
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        max_temp = None
        min_temp = None

        # Strategy 1: elements with class containing 'max'/'min'
        max_el = soup.find(class_=re.compile(r'max', re.I))
        min_el = soup.find(class_=re.compile(r'min', re.I))
        if max_el:
            temp_match = re.search(r'(-?\d+\.?\d*)', max_el.get_text())
            if temp_match:
                max_temp = float(temp_match.group(1))
        if min_el:
            temp_match = re.search(r'(-?\d+\.?\d*)', min_el.get_text())
            if temp_match:
                min_temp = float(temp_match.group(1))

        # Strategy 2: degree symbol patterns in page text
        if max_temp is None:
            temp_pattern = re.findall(r'(-?\d+)\s*°', soup.get_text())
            if len(temp_pattern) >= 2:
                temps = [int(t) for t in temp_pattern[:4]]
                min_temp = min(temps)
                max_temp = max(temps)

        if max_temp is None:
            print("  Weatherzone: Could not extract temperatures.")
            return None

        rain_prob = None
        rain_min = 0.0
        rain_max = 0.0

        prob_el = soup.find(string=re.compile(r'\d+\s*%'))
        if prob_el:
            prob_match = re.search(r'(\d+)\s*%', prob_el)
            if prob_match:
                rain_prob = float(prob_match.group(1))

        rain_text = soup.find(string=re.compile(r'\d+[\-–]\d+\s*mm'))
        if rain_text:
            matches = re.findall(r'(\d+\.?\d*)', rain_text)
            if len(matches) >= 2:
                rain_min = float(matches[0])
                rain_max = float(matches[1])
        else:
            rain_single = soup.find(string=re.compile(r'(\d+\.?\d*)\s*mm'))
            if rain_single:
                match = re.search(r'(\d+\.?\d*)\s*mm', rain_single)
                if match:
                    rain_min = float(match.group(1))
                    rain_max = rain_min

        print(f"  Weatherzone: Max={max_temp}, Min={min_temp}, "
              f"Rain={rain_min}-{rain_max}mm, Prob={rain_prob}%")

        return {
            'Forecast_Min_Temp': min_temp,
            'Forecast_Max_Temp': max_temp,
            'Forecast_Rain_Prob': rain_prob,
            'Forecast_Rain_Min_mm': rain_min,
            'Forecast_Rain_Max_mm': rain_max,
        }
    except requests.exceptions.RequestException as e:
        print(f"  Weatherzone HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"  Weatherzone Parse Error: {e}")
        return None


# ============================================================
#  3. COLLECT TODAY'S FORECASTS
# ============================================================

if not already_ran_today:
    for station_name, coords in STATIONS.items():
        print(f"\nFetching forecasts for {station_name}...")

        # --- BOM Published Forecast (location-specific) ---
        bom_data = fetch_bom_forecast(coords['bom_place'])
        if bom_data:
            new_records.append({
                'Date': today_str,
                'Station': station_name,
                'Source': 'BOM',
                **bom_data
            })
            print(f"  ✓ BOM")
        else:
            print(f"  ✗ BOM — failed")
        time.sleep(2)

        # --- Open-Meteo Best Match (location-specific via lat/lon) ---
        om_data = fetch_open_meteo(coords['lat'], coords['lon'])
        if om_data:
            new_records.append({
                'Date': today_str,
                'Station': station_name,
                'Source': 'Open-Meteo',
                **om_data
            })
            print(f"  ✓ Open-Meteo")
        else:
            print(f"  ✗ Open-Meteo — failed")
        time.sleep(1)

        # --- Weatherzone (Adelaide-wide, same for all stations) ---
        wz_data = scrape_weatherzone()
        if wz_data:
            new_records.append({
                'Date': today_str,
                'Station': station_name,
                'Source': 'Weatherzone',
                **wz_data
            })
            print(f"  ✓ Weatherzone")
        else:
            print(f"  ✗ Weatherzone — failed")
        time.sleep(1)

    print(f"\nCollected {len(new_records)} forecast records for {today_str}.")
else:
    print(f"Forecasts for {today_str} already recorded. Checking Actuals only.")


# ============================================================
#  4. UPDATE YESTERDAY'S ACTUALS (BOM Observations JSON)
# ============================================================

def fetch_bom_actuals(station_name, bom_id, prod_id):
    """
    Fetch actual observed weather from BOM JSON observation feed.

    This is NOT a forecast — it's the real recorded data from the
    weather station instruments. Used to grade all predictions.
    """
    url = f"http://www.bom.gov.au/fwo/{prod_id}/{prod_id}.{bom_id}.json"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'http://www.bom.gov.au/'
    }
    try:
        res = requests.get(url, headers=headers, timeout=15)

        if res.status_code != 200:
            print(f"  BOM Actuals {station_name}: HTTP {res.status_code}")
            return None

        data = res.json()['observations']['data']

        if not data or len(data) < 2:
            print(f"  BOM Actuals {station_name}: Insufficient observation data.")
            return None

        # Filter observations to yesterday's date
        yesterday_obs = []
        for obs in data:
            if obs.get('local_date_time_full'):
                obs_date = obs['local_date_time_full'][:8]  # YYYYMMDD
                yesterday_date = yesterday_str.replace('-', '')
                if obs_date == yesterday_date and obs.get('air_temp') is not None:
                    yesterday_obs.append(obs)

        if not yesterday_obs:
            # Fallback: use first 48 entries (approximately 24 hours)
            yesterday_obs = [x for x in data[:48] if x.get('air_temp') is not None]

        if not yesterday_obs:
            print(f"  BOM Actuals {station_name}: No valid temperature observations.")
            return None

        max_t = max(x['air_temp'] for x in yesterday_obs)
        min_t = min(x['air_temp'] for x in yesterday_obs)

        # Rain: 'rain_trace' is cumulative since 9am
        rain = data[0].get('rain_trace', '0')
        rain_mm = 0.0
        if rain and rain != '-':
            try:
                rain_mm = float(rain)
            except ValueError:
                rain_mm = 0.0

        print(f"  BOM Actuals {station_name}: Max={max_t}°C, Min={min_t}°C, Rain={rain_mm}mm")
        return {
            'Actual_Min_Temp': min_t,
            'Actual_Max_Temp': max_t,
            'Actual_Rain_mm': rain_mm
        }
    except requests.exceptions.RequestException as e:
        print(f"  BOM Actuals {station_name} HTTP Error: {e}")
        return None
    except (KeyError, IndexError, ValueError) as e:
        print(f"  BOM Actuals {station_name} Parse Error: {e}")
        return None


print(f"\nFetching BOM actuals for {yesterday_str}...")
for station_name, coords in STATIONS.items():
    actuals = fetch_bom_actuals(station_name, coords['bom_id'], coords['prod_id'])
    if actuals:
        mask = (df_history['Date'] == yesterday_str) & (df_history['Station'] == station_name)
        if mask.any():
            df_history.loc[mask, 'Actual_Min_Temp'] = actuals['Actual_Min_Temp']
            df_history.loc[mask, 'Actual_Max_Temp'] = actuals['Actual_Max_Temp']
            df_history.loc[mask, 'Actual_Rain_mm'] = actuals['Actual_Rain_mm']
            print(f"  ✓ Updated {mask.sum()} rows for {station_name}")
        else:
            print(f"  ⚠ No forecast rows for {station_name} on {yesterday_str} to update.")

    time.sleep(2)


# ============================================================
#  5. SAVE
# ============================================================

if new_records:
    df_new = pd.DataFrame(new_records)
    for col in CSV_COLUMNS:
        if col not in df_new.columns:
            df_new[col] = pd.NA
    df_new = df_new[CSV_COLUMNS]
    df_final = pd.concat([df_history, df_new], ignore_index=True)
else:
    df_final = df_history

df_final = df_final[CSV_COLUMNS]
df_final.to_csv(CSV_FILE, index=False)
print(f"\n✓ Pipeline complete. CSV has {len(df_final)} rows.")
