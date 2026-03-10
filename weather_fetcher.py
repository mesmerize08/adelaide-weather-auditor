"""
Adelaide Weather Accuracy Auditor — Data Fetcher
Runs daily via GitHub Actions at 9 AM Adelaide time.
Collects forecasts from BOM, Weatherzone, and Open-Meteo,
then backfills yesterday's actuals from BOM observations.
"""

import os
import sys
import json
import logging
import re
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────
adelaide_tz = pytz.timezone('Australia/Adelaide')
now = datetime.now(adelaide_tz)
today_str = now.strftime('%Y-%m-%d')
yesterday_str = (now - timedelta(days=1)).strftime('%Y-%m-%d')
CSV_FILE = 'weather_history.csv'

log.info(f"Running at Adelaide time: {now.strftime('%Y-%m-%d %H:%M %Z')}")
log.info(f"Collecting forecasts for: {today_str} | Actuals for: {yesterday_str}")

# Three stations as specified by the user
STATIONS = {
    'West Terrace': {
        'lat': -34.9285,
        'lon': 138.5955,
        'bom_station_id': '023034',   # BOM 6-digit station number (SA)
        'bom_search': 'Adelaide',      # Search term for BOM forecast API
        'wz_url': 'https://www.weatherzone.com.au/sa/adelaide/adelaide',
    },
    'Airport': {
        'lat': -34.9524,
        'lon': 138.5196,
        'bom_station_id': '023090',
        'bom_search': 'Adelaide Airport',
        'wz_url': 'https://www.weatherzone.com.au/sa/adelaide/adelaide-airport',
    },
    'Mt Lofty': {
        'lat': -34.9800,
        'lon': 138.7083,
        'bom_station_id': '023838',
        'bom_search': 'Mount Lofty',
        'wz_url': 'https://www.weatherzone.com.au/sa/mount-lofty/mount-lofty',
    },
}

COLUMNS = [
    'Date', 'Station', 'Source',
    'Forecast_Min_Temp', 'Forecast_Max_Temp',
    'Forecast_Rain_Prob', 'Forecast_Rain_Min_mm', 'Forecast_Rain_Max_mm',
    'Actual_Min_Temp', 'Actual_Max_Temp', 'Actual_Rain_mm',
]

# Browser-like headers to avoid 403s
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-AU,en;q=0.9',
}

# ─────────────────────────────────────────────────────────
# Initialize CSV
# ─────────────────────────────────────────────────────────
if not os.path.exists(CSV_FILE):
    pd.DataFrame(columns=COLUMNS).to_csv(CSV_FILE, index=False)
    log.info(f"Created new {CSV_FILE}")

df_history = pd.read_csv(CSV_FILE)
new_records = []


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────

def already_exists(station: str, source: str) -> bool:
    """Return True if today's record for this station+source already exists."""
    if df_history.empty:
        return False
    mask = (
        (df_history['Date'].astype(str) == today_str)
        & (df_history['Station'] == station)
        & (df_history['Source'] == source)
    )
    return bool(mask.any())


def safe_float(val, default=None):
    """Convert to float safely."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────
# FORECAST FETCHERS
# ─────────────────────────────────────────────────────────

def fetch_open_meteo(lat: float, lon: float) -> dict | None:
    """Open-Meteo free API — no API key required."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,"
        f"precipitation_probability_max,precipitation_sum"
        f"&timezone=Australia%2FAdelaide&forecast_days=1"
    )
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        d = res.json()['daily']
        return {
            'Min_Temp': d['temperature_2m_min'][0],
            'Max_Temp': d['temperature_2m_max'][0],
            'Rain_Prob': d['precipitation_probability_max'][0],
            'Rain_Min': d['precipitation_sum'][0],
            'Rain_Max': d['precipitation_sum'][0],
        }
    except Exception as exc:
        log.error(f"Open-Meteo ({lat},{lon}): {exc}")
        return None


def fetch_bom_forecast(search_term: str) -> dict | None:
    """
    BOM forecast via the new weather.bom.gov.au JSON API.
    Step 1: resolve location geohash via search endpoint.
    Step 2: fetch daily forecast for that geohash.
    """
    api_headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
    }
    try:
        # Step 1: resolve geohash
        search_url = (
            f"https://api.weather.bom.gov.au/v1/locations"
            f"?search={requests.utils.quote(search_term)}"
        )
        search_res = requests.get(search_url, headers=api_headers, timeout=15)
        search_res.raise_for_status()
        search_data = search_res.json().get('data', [])

        geohash = None
        for loc in search_data:
            if loc.get('state') == 'SA':
                geohash = loc.get('geohash')
                log.info(f"BOM: '{search_term}' → geohash={geohash}, name={loc.get('name')}")
                break

        if not geohash:
            log.warning(f"BOM: No SA result for '{search_term}'")
            return None

        # Step 2: daily forecast
        fc_url = f"https://api.weather.bom.gov.au/v1/locations/{geohash}/forecasts/daily"
        fc_res = requests.get(fc_url, headers=api_headers, timeout=15)
        fc_res.raise_for_status()
        today_data = fc_res.json().get('data', [{}])[0]

        rain_info = today_data.get('rain', {})
        amount_info = rain_info.get('amount', {})

        max_temp = today_data.get('temp_max')
        if max_temp is None:
            log.warning(f"BOM: Null max_temp for '{search_term}'")
            return None

        return {
            'Min_Temp': today_data.get('temp_min'),
            'Max_Temp': max_temp,
            'Rain_Prob': rain_info.get('chance'),
            'Rain_Min': amount_info.get('min') or 0,
            'Rain_Max': amount_info.get('max') or 0,
        }
    except Exception as exc:
        log.error(f"BOM forecast '{search_term}': {exc}")
        return None


def scrape_weatherzone(url: str) -> dict | None:
    """
    Scrape Weatherzone forecast page.
    Tries three strategies in order:
      1. Embedded Next.js JSON (__NEXT_DATA__)
      2. Structured schema.org / application/json script tags
      3. Regex-based text extraction from page body
    """
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # ── Strategy 1: Next.js embedded data ────────────────────
        nd_script = soup.find('script', id='__NEXT_DATA__')
        if nd_script:
            try:
                nd = json.loads(nd_script.string)
                page_props = nd.get('props', {}).get('pageProps', {})

                # Traverse common paths Weatherzone uses
                candidates = [
                    page_props.get('forecast'),
                    page_props.get('locationData', {}).get('forecast'),
                    page_props.get('weatherData', {}).get('forecast'),
                    page_props.get('forecastData'),
                    page_props.get('initialData', {}).get('forecast'),
                ]
                for fc in candidates:
                    if not fc:
                        continue
                    today = fc[0] if isinstance(fc, list) else fc
                    max_t = (
                        today.get('maxTemp') or today.get('max_temp') or
                        today.get('max') or today.get('high')
                    )
                    if max_t is not None:
                        min_t = (
                            today.get('minTemp') or today.get('min_temp') or
                            today.get('min') or today.get('low')
                        )
                        rain_prob = (
                            today.get('rainProb') or today.get('rain_probability') or
                            today.get('pop') or today.get('precip_probability')
                        )
                        rain_min_v = today.get('rainMin') or today.get('rain_min') or 0
                        rain_max_v = today.get('rainMax') or today.get('rain_max') or rain_min_v
                        log.info(f"WZ Next.js: max={max_t}, min={min_t}")
                        return {
                            'Min_Temp': safe_float(min_t),
                            'Max_Temp': safe_float(max_t),
                            'Rain_Prob': safe_float(rain_prob),
                            'Rain_Min': safe_float(rain_min_v, 0.0),
                            'Rain_Max': safe_float(rain_max_v, 0.0),
                        }
            except (json.JSONDecodeError, AttributeError) as exc:
                log.debug(f"WZ Next.js parse fail: {exc}")

        # ── Strategy 2: application/json script tags ──────────────
        for tag in soup.find_all('script', type='application/json'):
            try:
                data = json.loads(tag.string or '')
                # Look for temperature-like keys anywhere in the structure
                text = json.dumps(data)
                if 'maxTemp' in text or 'max_temp' in text or 'temperature' in text.lower():
                    # Try to walk the dict for forecast data
                    # (structure varies greatly; this is a best-effort)
                    pass
            except Exception:
                pass

        # ── Strategy 3: Regex on page text ────────────────────────
        page_text = soup.get_text(separator=' ')

        max_match = re.search(r'\bMax(?:imum)?\s*:?\s*(\d{1,2})\s*°', page_text, re.IGNORECASE)
        min_match = re.search(r'\bMin(?:imum)?\s*:?\s*(\d{1,2})\s*°', page_text, re.IGNORECASE)

        # Rain probability: "30% chance" or "Chance of rain: 30%"
        rain_prob_match = re.search(
            r'(\d{1,3})\s*%\s*(?:chance|probability|pop)',
            page_text, re.IGNORECASE
        )
        # Rain amount: "1-5mm", "0 to 5mm", "5mm"
        rain_range_match = re.search(r'(\d+(?:\.\d+)?)\s*[-–to]+\s*(\d+(?:\.\d+)?)\s*mm', page_text, re.IGNORECASE)
        rain_single_match = re.search(r'(\d+(?:\.\d+)?)\s*mm', page_text, re.IGNORECASE)

        if max_match:
            min_t = safe_float(min_match.group(1)) if min_match else None
            max_t = safe_float(max_match.group(1))
            rain_prob = safe_float(rain_prob_match.group(1)) if rain_prob_match else None

            if rain_range_match:
                rain_min_v = safe_float(rain_range_match.group(1), 0.0)
                rain_max_v = safe_float(rain_range_match.group(2), 0.0)
            elif rain_single_match:
                rain_min_v = rain_max_v = safe_float(rain_single_match.group(1), 0.0)
            else:
                rain_min_v = rain_max_v = 0.0

            log.info(f"WZ regex: max={max_t}, min={min_t}")
            return {
                'Min_Temp': min_t,
                'Max_Temp': max_t,
                'Rain_Prob': rain_prob,
                'Rain_Min': rain_min_v,
                'Rain_Max': rain_max_v,
            }

        log.warning(f"WZ: All strategies failed for {url}")
        return None

    except requests.HTTPError as exc:
        log.error(f"WZ HTTP error {url}: {exc}")
        return None
    except Exception as exc:
        log.error(f"WZ scrape error {url}: {exc}")
        return None


# ─────────────────────────────────────────────────────────
# ACTUALS FETCHER
# ─────────────────────────────────────────────────────────

def fetch_bom_actuals(station_id: str) -> dict | None:
    """
    Fetch actuals from BOM observation JSON (IDS60901 SA product).
    Returns max/min temp and total rainfall for the most recent ~24h period.
    Tries 6-digit station ID first, then 5-digit (WMO) as fallback.
    """
    ids_to_try = [
        f"http://www.bom.gov.au/fwo/IDS60901/IDS60901.{station_id}.json",
        # Fallback: strip leading zero for stations like 023034 → 23034
        f"http://www.bom.gov.au/fwo/IDS60901/IDS60901.{station_id.lstrip('0')}.json",
    ]

    for url in ids_to_try:
        try:
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
            res.raise_for_status()
            data = res.json()['observations']['data']

            if not data:
                continue

            # BOM data is in 30-min intervals, newest first.
            # Take up to 48 records (~24h) to find max/min.
            readings = data[:48]
            temps = [
                x['air_temp'] for x in readings
                if x.get('air_temp') is not None
            ]

            if not temps:
                continue

            # rain_trace = cumulative mm since 9am local time (newest reading)
            rain_raw = str(data[0].get('rain_trace', '-')).strip()
            if rain_raw in ('-', '', 'None'):
                rain_mm = 0.0
            elif rain_raw.lower() == 'trace':
                rain_mm = 0.1  # BOM "Trace" = <0.2mm
            else:
                rain_mm = safe_float(rain_raw, 0.0)

            return {
                'Actual_Min_Temp': min(temps),
                'Actual_Max_Temp': max(temps),
                'Actual_Rain_mm': rain_mm,
            }

        except Exception as exc:
            log.debug(f"BOM obs {url}: {exc}")
            continue

    log.error(f"BOM actuals: all URLs failed for station_id={station_id}")
    return None


# ─────────────────────────────────────────────────────────
# STEP 1 — Fetch today's forecasts
# ─────────────────────────────────────────────────────────
log.info("=" * 60)
log.info("STEP 1: Fetching forecasts")

for name, cfg in STATIONS.items():

    # --- Open-Meteo ---
    if already_exists(name, 'Open-Meteo'):
        log.info(f"  Open-Meteo | {name}: already recorded, skipping")
    else:
        om = fetch_open_meteo(cfg['lat'], cfg['lon'])
        if om:
            new_records.append({
                'Date': today_str, 'Station': name, 'Source': 'Open-Meteo',
                'Forecast_Min_Temp': om['Min_Temp'],
                'Forecast_Max_Temp': om['Max_Temp'],
                'Forecast_Rain_Prob': om['Rain_Prob'],
                'Forecast_Rain_Min_mm': om['Rain_Min'],
                'Forecast_Rain_Max_mm': om['Rain_Max'],
            })
            log.info(
                f"  Open-Meteo | {name}: "
                f"Max {om['Max_Temp']}°C  Min {om['Min_Temp']}°C  "
                f"Rain {om['Rain_Min']}–{om['Rain_Max']}mm  ({om['Rain_Prob']}%)"
            )
        else:
            log.warning(f"  Open-Meteo | {name}: FAILED")

    # --- BOM ---
    if already_exists(name, 'BOM'):
        log.info(f"  BOM        | {name}: already recorded, skipping")
    else:
        bom_fc = fetch_bom_forecast(cfg['bom_search'])
        if bom_fc:
            new_records.append({
                'Date': today_str, 'Station': name, 'Source': 'BOM',
                'Forecast_Min_Temp': bom_fc['Min_Temp'],
                'Forecast_Max_Temp': bom_fc['Max_Temp'],
                'Forecast_Rain_Prob': bom_fc['Rain_Prob'],
                'Forecast_Rain_Min_mm': bom_fc['Rain_Min'],
                'Forecast_Rain_Max_mm': bom_fc['Rain_Max'],
            })
            log.info(
                f"  BOM        | {name}: "
                f"Max {bom_fc['Max_Temp']}°C  Min {bom_fc['Min_Temp']}°C  "
                f"Rain {bom_fc['Rain_Min']}–{bom_fc['Rain_Max']}mm  ({bom_fc['Rain_Prob']}%)"
            )
        else:
            log.warning(f"  BOM        | {name}: FAILED")

    # --- Weatherzone ---
    if already_exists(name, 'Weatherzone'):
        log.info(f"  Weatherzone| {name}: already recorded, skipping")
    else:
        wz = scrape_weatherzone(cfg['wz_url'])
        if wz:
            new_records.append({
                'Date': today_str, 'Station': name, 'Source': 'Weatherzone',
                'Forecast_Min_Temp': wz['Min_Temp'],
                'Forecast_Max_Temp': wz['Max_Temp'],
                'Forecast_Rain_Prob': wz['Rain_Prob'],
                'Forecast_Rain_Min_mm': wz['Rain_Min'],
                'Forecast_Rain_Max_mm': wz['Rain_Max'],
            })
            log.info(
                f"  Weatherzone| {name}: "
                f"Max {wz['Max_Temp']}°C  Min {wz['Min_Temp']}°C  "
                f"Rain {wz['Rain_Min']}–{wz['Rain_Max']}mm  ({wz['Rain_Prob']}%)"
            )
        else:
            log.warning(f"  Weatherzone| {name}: FAILED")


# ─────────────────────────────────────────────────────────
# STEP 2 — Update yesterday's actuals from BOM observations
# ─────────────────────────────────────────────────────────
log.info("=" * 60)
log.info("STEP 2: Backfilling actuals")

for name, cfg in STATIONS.items():
    actuals = fetch_bom_actuals(cfg['bom_station_id'])
    if actuals:
        mask = (
            (df_history['Date'].astype(str) == yesterday_str)
            & (df_history['Station'] == name)
        )
        rows_updated = mask.sum()
        if rows_updated:
            df_history.loc[mask, 'Actual_Min_Temp'] = actuals['Actual_Min_Temp']
            df_history.loc[mask, 'Actual_Max_Temp'] = actuals['Actual_Max_Temp']
            df_history.loc[mask, 'Actual_Rain_mm'] = actuals['Actual_Rain_mm']
            log.info(
                f"  {name}: updated {rows_updated} rows — "
                f"Max {actuals['Actual_Max_Temp']}°C  "
                f"Min {actuals['Actual_Min_Temp']}°C  "
                f"Rain {actuals['Actual_Rain_mm']}mm"
            )
        else:
            log.warning(f"  {name}: no existing rows for {yesterday_str} to backfill")
    else:
        log.warning(f"  {name}: actuals fetch FAILED")


# ─────────────────────────────────────────────────────────
# STEP 3 — Save to CSV
# ─────────────────────────────────────────────────────────
log.info("=" * 60)

if new_records:
    df_new = pd.DataFrame(new_records)
    for col in COLUMNS:
        if col not in df_new.columns:
            df_new[col] = None
    df_final = pd.concat([df_history, df_new[COLUMNS]], ignore_index=True)
    log.info(f"STEP 3: Adding {len(new_records)} new records")
else:
    df_final = df_history
    log.info("STEP 3: No new records (all existed or all fetches failed)")

df_final.to_csv(CSV_FILE, index=False)
log.info(f"Saved {CSV_FILE} — {len(df_final)} total rows")
