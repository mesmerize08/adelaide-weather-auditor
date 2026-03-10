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
        # BOM observation JSON uses WMO IDs in filenames; 6-digit BOM station as fallback
        'bom_wmo_id': '94648',
        'bom_station_id': '023034',
        # BOM forecast API: confirmed geohash from live run (r1f93ck)
        'bom_search': 'Adelaide',
        'bom_geohash': 'r1f93ck',
        # Weatherzone: all three stations use Adelaide city forecast (WZ is city-level)
        'wz_url': 'https://www.weatherzone.com.au/sa/adelaide/adelaide',
    },
    'Airport': {
        'lat': -34.9524,
        'lon': 138.5196,
        'bom_wmo_id': '94672',
        'bom_station_id': '023090',
        # BOM forecast API: confirmed geohash from live run (r1f90q5)
        'bom_search': 'Adelaide Airport',
        'bom_geohash': 'r1f90q5',
        'wz_url': 'https://www.weatherzone.com.au/sa/adelaide/adelaide',
    },
    'Mt Lofty': {
        'lat': -34.9800,
        'lon': 138.7083,
        'bom_wmo_id': '94693',
        'bom_station_id': '023838',
        # BOM search API returns no result for "Mount Lofty" (summit, not a suburb).
        # Use hardcoded geohash (r1fy9t) — verified approximate for -34.98, 138.71.
        'bom_search': 'Mount Lofty',
        'bom_geohash': 'r1fy9t',
        'wz_url': 'https://www.weatherzone.com.au/sa/adelaide/adelaide',
    },
}

COLUMNS = [
    'Date', 'Station', 'Source',
    'Forecast_Min_Temp', 'Forecast_Max_Temp',
    'Forecast_Rain_Prob', 'Forecast_Rain_Min_mm', 'Forecast_Rain_Max_mm',
    'Actual_Min_Temp', 'Actual_Max_Temp', 'Actual_Rain_mm',
]

# Full Chrome-like headers including Sec-Fetch-* which many CDNs/Cloudflare check
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': (
        'text/html,application/xhtml+xml,application/xml;'
        'q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'
    ),
    'Accept-Language': 'en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
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


def fetch_bom_forecast(search_term: str, known_geohash: str | None = None) -> dict | None:
    """
    BOM forecast via the new weather.bom.gov.au JSON API.
    If known_geohash is provided it is used directly (skips search).
    Otherwise: search for an SA location, fall back to known_geohash if nothing found.
    """
    api_headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
    }

    geohash = known_geohash  # may be overwritten by a fresher search result

    try:
        # ── Step 1: resolve geohash via search (best-effort) ──────
        search_url = (
            f"https://api.weather.bom.gov.au/v1/locations"
            f"?search={requests.utils.quote(search_term)}"
        )
        search_res = requests.get(search_url, headers=api_headers, timeout=15)
        search_res.raise_for_status()
        search_data = search_res.json().get('data', [])

        for loc in search_data:
            if loc.get('state') == 'SA':
                geohash = loc.get('geohash')
                log.info(f"BOM: '{search_term}' → geohash={geohash}, name={loc.get('name')}")
                break

        if geohash == known_geohash and known_geohash:
            log.info(f"BOM: No live SA result for '{search_term}', using hardcoded geohash={geohash}")
        elif not geohash:
            log.warning(f"BOM: No geohash resolved for '{search_term}' — skipping")
            return None

        # ── Step 2: daily forecast ─────────────────────────────────
        fc_url = f"https://api.weather.bom.gov.au/v1/locations/{geohash}/forecasts/daily"
        fc_res = requests.get(fc_url, headers=api_headers, timeout=15)
        fc_res.raise_for_status()
        today_data = fc_res.json().get('data', [{}])[0]

        rain_info = today_data.get('rain', {})
        amount_info = rain_info.get('amount', {})

        max_temp = today_data.get('temp_max')
        if max_temp is None:
            log.warning(f"BOM: Null max_temp for geohash={geohash}")
            return None

        return {
            'Min_Temp': today_data.get('temp_min'),
            'Max_Temp': max_temp,
            'Rain_Prob': rain_info.get('chance'),
            'Rain_Min': amount_info.get('min') or 0,
            'Rain_Max': amount_info.get('max') or 0,
        }
    except Exception as exc:
        log.error(f"BOM forecast '{search_term}' (geohash={geohash}): {exc}")
        return None


def scrape_weatherzone(url: str) -> dict | None:
    """
    Scrape Weatherzone Adelaide forecast.
    Tries the desktop URL first, then a mobile URL fallback.
    Within each page, tries three parsing strategies:
      1. Embedded Next.js JSON (__NEXT_DATA__)
      2. application/json script tags
      3. Regex-based text extraction from page body
    """
    # Build list of URLs to attempt: desktop first, then mobile subdomain
    mobile_url = url.replace(
        'www.weatherzone.com.au', 'm.weatherzone.com.au'
    )
    urls_to_try = [url, mobile_url]

    for attempt_url in urls_to_try:
        result = _parse_weatherzone_page(attempt_url)
        if result:
            return result

    log.warning(f"WZ: All strategies failed for {url} (desktop + mobile)")
    return None


def _parse_weatherzone_page(url: str) -> dict | None:
    """Fetch one Weatherzone URL and attempt to extract forecast data."""
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)

        # Log HTTP status so we can diagnose blocks/redirects in Actions logs
        log.info(f"WZ HTTP {res.status_code} → {res.url}")
        res.raise_for_status()

        content_type = res.headers.get('Content-Type', '')
        if 'text/html' not in content_type and 'json' not in content_type:
            log.warning(f"WZ: Unexpected Content-Type '{content_type}' from {url}")
            return None

        # If the response is JSON directly (some endpoints return JSON)
        if 'json' in content_type:
            try:
                return _extract_from_wz_json(res.json())
            except Exception:
                pass

        soup = BeautifulSoup(res.text, 'html.parser')

        # Diagnostic: report whether Next.js data was embedded
        nd_script = soup.find('script', id='__NEXT_DATA__')
        log.info(f"WZ __NEXT_DATA__ present: {nd_script is not None}")
        if nd_script:
            log.debug(f"WZ __NEXT_DATA__ (first 300 chars): {nd_script.string[:300] if nd_script.string else 'empty'}")

        # ── Strategy 1: Next.js embedded data ────────────────────
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

        log.info(f"WZ: No data extracted from {url}")
        return None

    except requests.HTTPError as exc:
        log.warning(f"WZ HTTP error {url}: {exc}")
        return None
    except Exception as exc:
        log.warning(f"WZ parse error {url}: {exc}")
        return None


def _extract_from_wz_json(data: dict) -> dict | None:
    """
    Attempt to extract today's forecast from a Weatherzone JSON payload.
    Called when the response Content-Type is application/json.
    """
    try:
        # Try common Weatherzone API response shapes
        forecasts = (
            data.get('forecasts') or
            data.get('forecast') or
            data.get('data', {}).get('forecasts') or
            []
        )
        today = forecasts[0] if isinstance(forecasts, list) and forecasts else {}
        max_t = today.get('maxTemp') or today.get('max') or today.get('tempMax')
        if max_t is None:
            return None
        min_t = today.get('minTemp') or today.get('min') or today.get('tempMin')
        rain_prob = today.get('rainProb') or today.get('pop') or today.get('rainChance')
        rain_min_v = today.get('rainMin') or today.get('precipMin') or 0
        rain_max_v = today.get('rainMax') or today.get('precipMax') or rain_min_v
        log.info(f"WZ JSON API: max={max_t}, min={min_t}")
        return {
            'Min_Temp': safe_float(min_t),
            'Max_Temp': safe_float(max_t),
            'Rain_Prob': safe_float(rain_prob),
            'Rain_Min': safe_float(rain_min_v, 0.0),
            'Rain_Max': safe_float(rain_max_v, 0.0),
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────
# ACTUALS FETCHER
# ─────────────────────────────────────────────────────────

def fetch_bom_actuals(wmo_id: str, station_id: str) -> dict | None:
    """
    Fetch actuals from BOM observation JSON (IDS60901 SA product).
    The IDS60901 filenames use the WMO ID (5-digit, e.g. 94648).
    Falls back to Open-Meteo historical if BOM is unreachable (e.g. IP geo-block).
    """
    # WMO ID first — this is what IDS60901 filenames actually use
    urls_to_try = [
        f"http://www.bom.gov.au/fwo/IDS60901/IDS60901.{wmo_id}.json",
        f"http://www.bom.gov.au/fwo/IDS60901/IDS60901.{station_id}.json",
        f"http://reg.bom.gov.au/fwo/IDS60901/IDS60901.{wmo_id}.json",
    ]

    for url in urls_to_try:
        try:
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
            log.info(f"BOM obs HTTP {res.status_code} → {url}")
            res.raise_for_status()
            data = res.json()['observations']['data']

            if not data:
                continue

            # BOM data is 30-min intervals, newest first. Take up to 48 (~24h).
            readings = data[:48]
            temps = [x['air_temp'] for x in readings if x.get('air_temp') is not None]
            if not temps:
                continue

            # rain_trace = cumulative mm since 9am local time
            rain_raw = str(data[0].get('rain_trace', '-')).strip()
            if rain_raw in ('-', '', 'None'):
                rain_mm = 0.0
            elif rain_raw.lower() == 'trace':
                rain_mm = 0.1
            else:
                rain_mm = safe_float(rain_raw, 0.0)

            return {
                'Actual_Min_Temp': min(temps),
                'Actual_Max_Temp': max(temps),
                'Actual_Rain_mm': rain_mm,
            }

        except Exception as exc:
            log.warning(f"BOM obs attempt failed ({url}): {exc}")
            continue

    # ── Fallback: Open-Meteo provides model-analysis data for recent past days ──
    log.warning(f"BOM actuals unavailable for WMO={wmo_id} — trying Open-Meteo fallback")
    return None  # fallback is invoked per-station in the main loop


def fetch_open_meteo_actuals(lat: float, lon: float, date_str: str) -> dict | None:
    """
    Open-Meteo fallback for yesterday's actuals using their model-analysis data.
    Uses past_days so no ERA5 lag issues — analysis data is available same-day.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=Australia%2FAdelaide"
        f"&start_date={date_str}&end_date={date_str}"
    )
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        d = res.json().get('daily', {})
        max_t = d.get('temperature_2m_max', [None])[0]
        min_t = d.get('temperature_2m_min', [None])[0]
        rain = d.get('precipitation_sum', [None])[0]
        if max_t is None:
            return None
        log.info(
            f"Open-Meteo actuals ({lat},{lon}) {date_str}: "
            f"Max {max_t}°C  Min {min_t}°C  Rain {rain}mm"
        )
        return {
            'Actual_Min_Temp': min_t,
            'Actual_Max_Temp': max_t,
            'Actual_Rain_mm': rain if rain is not None else 0.0,
        }
    except Exception as exc:
        log.error(f"Open-Meteo actuals fallback failed: {exc}")
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
        bom_fc = fetch_bom_forecast(cfg['bom_search'], known_geohash=cfg.get('bom_geohash'))
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
    # Try BOM observations; fall back to Open-Meteo if BOM is unreachable
    actuals = fetch_bom_actuals(cfg['bom_wmo_id'], cfg['bom_station_id'])
    if not actuals:
        log.warning(f"  {name}: BOM actuals failed — using Open-Meteo fallback")
        actuals = fetch_open_meteo_actuals(cfg['lat'], cfg['lon'], yesterday_str)

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
        log.error(f"  {name}: ALL actuals sources failed")


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
