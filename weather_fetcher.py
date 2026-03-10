"""
Adelaide Weather Accuracy Auditor — Data Fetcher
Runs daily via GitHub Actions at 9 AM Adelaide time.
Collects forecasts from BOM, Weatherzone, and Open-Meteo,
then backfills yesterday's actuals from BOM observations.
"""

import os
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

def validate_forecast(data: dict, source: str, station: str) -> bool:
    """
    Sanity-check a forecast dict before writing to CSV.
    Returns False (and logs a warning) if anything looks wrong.
    """
    min_t = safe_float(data.get('Min_Temp'))
    max_t = safe_float(data.get('Max_Temp'))
    rain_prob = safe_float(data.get('Rain_Prob'))
    rain_min = safe_float(data.get('Rain_Min'), 0.0)
    rain_max = safe_float(data.get('Rain_Max'), 0.0)

    if max_t is None:
        log.warning(f"Validation [{source}|{station}]: Max_Temp is None — skipping")
        return False
    if not (0 <= max_t <= 55):
        log.warning(f"Validation [{source}|{station}]: Max_Temp={max_t} outside [0,55] — skipping")
        return False
    if min_t is not None:
        if not (0 <= min_t <= 55):
            log.warning(f"Validation [{source}|{station}]: Min_Temp={min_t} outside [0,55] — skipping")
            return False
        if min_t > max_t:
            log.warning(f"Validation [{source}|{station}]: Min_Temp={min_t} > Max_Temp={max_t} — skipping")
            return False
    if rain_prob is not None and not (0 <= rain_prob <= 100):
        log.warning(f"Validation [{source}|{station}]: Rain_Prob={rain_prob} outside [0,100] — skipping")
        return False
    if rain_min > rain_max:
        log.warning(f"Validation [{source}|{station}]: Rain_Min={rain_min} > Rain_Max={rain_max} — skipping")
        return False
    return True


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

    Attempt order (each falls through to next on failure):
      1. Scrapling StealthyFetcher — real browser + Cloudflare Turnstile solver
      2. Playwright — headless Chromium with API response interception
      3. requests — plain HTTP (fastest, but usually blocked by Cloudflare)
    """
    # ── Primary: Scrapling (Cloudflare bypass) ─────────────────
    result = _scrape_weatherzone_scrapling(url)
    if result:
        return result

    # ── Secondary: Playwright ──────────────────────────────────
    result = _scrape_weatherzone_playwright(url)
    if result:
        return result

    # ── Fallback: plain HTTP ───────────────────────────────────
    log.info("WZ: All browser methods failed — trying requests fallback")
    for attempt_url in [url, url.replace('www.', 'm.')]:
        result = _scrape_weatherzone_requests(attempt_url)
        if result:
            return result

    log.warning(f"WZ: All methods failed for {url}")
    return None


# ─────────────────────────────────────────────────────────
# Weatherzone — Scrapling stealth renderer (primary)
# ─────────────────────────────────────────────────────────

def _scrape_weatherzone_scrapling(url: str) -> dict | None:
    """
    Use Scrapling's StealthyFetcher to render Weatherzone and bypass Cloudflare.
    StealthyFetcher launches a real browser with fingerprint spoofing and
    automatically solves Cloudflare Turnstile / interstitial challenges.

    Requires: pip install "scrapling[fetchers]" && scrapling install
    """
    try:
        from scrapling.fetchers import StealthyFetcher
    except ImportError:
        log.warning("WZ Scrapling: package not installed — skipping")
        return None

    try:
        page = StealthyFetcher.fetch(
            url,
            headless=True,
            network_idle=True,       # wait for React/XHR to settle
            solve_cloudflare=True,   # auto-solve Turnstile / interstitial
            disable_resources=False, # keep JS enabled for SPA rendering
        )

        if not page:
            log.warning("WZ Scrapling: empty response")
            return None

        # Get raw HTML from the Scrapling adaptor
        html = getattr(page, 'html', None) or str(page)
        if not html or len(html) < 500:
            log.warning("WZ Scrapling: response too short — likely blocked")
            return None

        log.info(f"WZ Scrapling: page fetched OK ({len(html):,} chars)")
        soup = BeautifulSoup(html, 'html.parser')

        # Strategy 1: __NEXT_DATA__ SSR blob
        nd_script = soup.find('script', id='__NEXT_DATA__')
        if nd_script and nd_script.string:
            try:
                nd = json.loads(nd_script.string)
                fc = _find_wz_forecast(nd)
                if fc:
                    result = _build_wz_result(fc)
                    if result:
                        log.info(f"WZ Scrapling __NEXT_DATA__: Max {result['Max_Temp']}°C")
                        return result
            except Exception as exc:
                log.debug(f"WZ Scrapling __NEXT_DATA__ error: {exc}")

        # Strategy 2: any application/json script tags
        for tag in soup.find_all('script', type='application/json'):
            try:
                fc = _find_wz_forecast(json.loads(tag.string or ''))
                if fc:
                    result = _build_wz_result(fc)
                    if result:
                        log.info(f"WZ Scrapling JSON tag: Max {result['Max_Temp']}°C")
                        return result
            except Exception:
                pass

        # Strategy 3: rendered DOM text regex
        result = _extract_wz_from_text(soup.get_text(separator=' '))
        if result:
            log.info(f"WZ Scrapling DOM text: Max {result['Max_Temp']}°C")
            return result

        log.warning("WZ Scrapling: page rendered but no forecast data found")
        return None

    except Exception as exc:
        log.error(f"WZ Scrapling error: {exc}")
        return None


# ─────────────────────────────────────────────────────────
# Weatherzone — Playwright renderer
# ─────────────────────────────────────────────────────────

def _scrape_weatherzone_playwright(url: str) -> dict | None:
    """
    Launch a headless Chromium browser via Playwright, fully render the
    Weatherzone React SPA, and extract today's forecast using three strategies:
      1. window.__NEXT_DATA__ (SSR JSON embedded in the page)
      2. Intercepted JSON API responses (XHR/fetch calls the React app makes)
      3. Rendered DOM text extraction (last resort)

    Requires: pip install playwright && playwright install chromium --with-deps
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.warning("WZ Playwright: package not installed — run 'pip install playwright'")
        return None

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled',  # avoid bot fingerprint
                    '--disable-dev-shm-usage',
                ],
            )
            context = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/122.0.0.0 Safari/537.36'
                ),
                locale='en-AU',
                timezone_id='Australia/Adelaide',
                # Pretend to be a real viewport so mobile-redirect doesn't trigger
                viewport={'width': 1280, 'height': 800},
            )
            page = context.new_page()

            # ── Intercept JSON API calls made by the React app ────
            api_responses = []

            def _on_response(response):
                if response.status != 200:
                    return
                ct = response.headers.get('content-type', '')
                if 'json' not in ct:
                    return
                url_lower = response.url.lower()
                # Capture any JSON response that looks weather-related
                if any(k in url_lower for k in ('forecast', 'weather', 'daily', 'location')):
                    try:
                        api_responses.append(response.json())
                    except Exception:
                        pass

            page.on('response', _on_response)

            # ── Load the page ──────────────────────────────────────
            try:
                page.goto(url, wait_until='networkidle', timeout=35_000)
            except PWTimeout:
                log.warning("WZ Playwright: networkidle timeout — retrying with domcontentloaded")
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=20_000)
                    page.wait_for_timeout(4_000)  # let React hydrate
                except PWTimeout:
                    log.warning("WZ Playwright: domcontentloaded timeout — aborting")
                    browser.close()
                    return None

            log.info(f"WZ Playwright: page loaded OK ({page.url})")

            # ── Strategy 1: window.__NEXT_DATA__ ──────────────────
            try:
                nd_raw = page.evaluate('() => JSON.stringify(window.__NEXT_DATA__ || null)')
                if nd_raw:
                    nd = json.loads(nd_raw)
                    fc = _find_wz_forecast(nd)
                    if fc:
                        result = _build_wz_result(fc)
                        if result:
                            log.info(f"WZ Playwright __NEXT_DATA__: Max {result['Max_Temp']}°C")
                            browser.close()
                            return result
                    log.debug("WZ Playwright: __NEXT_DATA__ present but no forecast found inside")
            except Exception as exc:
                log.debug(f"WZ Playwright __NEXT_DATA__ error: {exc}")

            # ── Strategy 2: intercepted API responses ──────────────
            for payload in api_responses:
                fc = _find_wz_forecast(payload)
                if fc:
                    result = _build_wz_result(fc)
                    if result:
                        log.info(f"WZ Playwright API intercept: Max {result['Max_Temp']}°C")
                        browser.close()
                        return result

            # ── Strategy 3: rendered DOM text ──────────────────────
            try:
                rendered_html = page.content()
                soup = BeautifulSoup(rendered_html, 'html.parser')
                result = _extract_wz_from_text(soup.get_text(separator=' '))
                if result:
                    log.info(f"WZ Playwright DOM text: Max {result['Max_Temp']}°C")
                    browser.close()
                    return result
            except Exception as exc:
                log.debug(f"WZ Playwright DOM error: {exc}")

            log.warning(f"WZ Playwright: rendered page yielded no data for {url}")
            browser.close()
            return None

        except Exception as exc:
            log.error(f"WZ Playwright fatal error: {exc}")
            try:
                browser.close()
            except Exception:
                pass
            return None


def _find_wz_forecast(data, _depth: int = 0):
    """
    Recursively search any JSON structure for a dict that looks like a
    daily weather forecast entry (has a recognisable max-temperature key).
    Returns the first match — assumes that is today's forecast.
    """
    if _depth > 10:
        return None

    if isinstance(data, dict):
        # Keys that unambiguously indicate a per-day forecast entry
        temp_keys = {
            'maxTemp', 'max_temp', 'tempMax', 'highTemperature',
            'maximumTemperature', 'temperature_max',
        }
        if temp_keys & set(data.keys()):
            val = next(data[k] for k in temp_keys if k in data)
            # Sanity-check: Adelaide max temps are 0–50°C
            if val is not None and 0 <= safe_float(val, -999) <= 55:
                return data

        for v in data.values():
            found = _find_wz_forecast(v, _depth + 1)
            if found:
                return found

    elif isinstance(data, list):
        for item in data[:5]:   # first item = today
            found = _find_wz_forecast(item, _depth + 1)
            if found:
                return found

    return None


def _first_not_none(d: dict, *keys):
    """Return the first value in `d` for any key in `keys` that is not None."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


def _build_wz_result(fc: dict) -> dict | None:
    """Convert a raw Weatherzone forecast dict to our standard result shape."""
    # Use _first_not_none so that a legitimate 0°C value isn't skipped by `or`
    max_t = _first_not_none(
        fc, 'maxTemp', 'max_temp', 'tempMax',
        'highTemperature', 'maximumTemperature', 'temperature_max',
    )
    if max_t is None:
        return None

    min_t = _first_not_none(
        fc, 'minTemp', 'min_temp', 'tempMin',
        'lowTemperature', 'minimumTemperature', 'temperature_min',
    )

    rain_prob = _first_not_none(
        fc, 'rainChance', 'rainProb', 'pop',
        'precipProbability', 'rain_probability',
        'chanceOfRain', 'precipitationProbability',
    )

    # Handle nested rain structure: {"rain": {"amount": {"min":1,"max":5}, "chance":30}}
    rain_info = _first_not_none(fc, 'rain', 'rainfall', 'precipitation') or {}
    if isinstance(rain_info, dict):
        if rain_prob is None:
            rain_prob = _first_not_none(rain_info, 'chance', 'probability')
        amount = rain_info.get('amount') or rain_info
        if isinstance(amount, dict):
            rain_min_v = amount.get('min', 0) or 0
            rain_max_v = amount.get('max', 0) or 0
        elif isinstance(amount, (int, float)):
            rain_min_v = rain_max_v = float(amount)
        else:
            rain_min_v = rain_max_v = 0.0
    else:
        rain_min_v = safe_float(_first_not_none(fc, 'rainMin', 'precipMin', 'rain_min'), 0.0)
        rain_max_v = safe_float(_first_not_none(fc, 'rainMax', 'precipMax', 'rain_max'), rain_min_v)

    return {
        'Min_Temp': safe_float(min_t),
        'Max_Temp': safe_float(max_t),
        'Rain_Prob': safe_float(rain_prob),
        'Rain_Min': rain_min_v,
        'Rain_Max': rain_max_v,
    }


# ─────────────────────────────────────────────────────────
# Weatherzone — requests-based fallback
# ─────────────────────────────────────────────────────────

def _scrape_weatherzone_requests(url: str) -> dict | None:
    """
    Attempt to fetch Weatherzone via plain HTTP.
    Often blocked by Cloudflare on GitHub Actions IPs, but worth trying.
    """
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        log.info(f"WZ requests HTTP {res.status_code} → {res.url}")
        res.raise_for_status()

        content_type = res.headers.get('Content-Type', '')

        # Direct JSON response
        if 'json' in content_type:
            try:
                fc = _find_wz_forecast(res.json())
                if fc:
                    return _build_wz_result(fc)
            except Exception:
                pass
            return None

        if 'text/html' not in content_type:
            log.warning(f"WZ requests: unexpected Content-Type '{content_type}'")
            return None

        soup = BeautifulSoup(res.text, 'html.parser')

        # Try __NEXT_DATA__ (works if server does SSR)
        nd_script = soup.find('script', id='__NEXT_DATA__')
        log.info(f"WZ requests: __NEXT_DATA__ present = {nd_script is not None}")
        if nd_script:
            try:
                nd = json.loads(nd_script.string or '')
                fc = _find_wz_forecast(nd)
                if fc:
                    result = _build_wz_result(fc)
                    if result:
                        log.info(f"WZ requests __NEXT_DATA__: Max {result['Max_Temp']}°C")
                        return result
            except Exception as exc:
                log.debug(f"WZ requests __NEXT_DATA__ error: {exc}")

        # Try any application/json script tags
        for tag in soup.find_all('script', type='application/json'):
            try:
                fc = _find_wz_forecast(json.loads(tag.string or ''))
                if fc:
                    result = _build_wz_result(fc)
                    if result:
                        log.info(f"WZ requests JSON tag: Max {result['Max_Temp']}°C")
                        return result
            except Exception:
                pass

        # Regex on plain text (last resort)
        return _extract_wz_from_text(soup.get_text(separator=' '))

    except requests.HTTPError as exc:
        log.warning(f"WZ requests HTTP error {url}: {exc}")
        return None
    except Exception as exc:
        log.warning(f"WZ requests error {url}: {exc}")
        return None


def _extract_wz_from_text(page_text: str) -> dict | None:
    """Regex-based extraction from plain rendered text. Last resort."""
    max_match = re.search(r'\bMax(?:imum)?\s*:?\s*(\d{1,2})\s*°', page_text, re.IGNORECASE)
    min_match = re.search(r'\bMin(?:imum)?\s*:?\s*(\d{1,2})\s*°', page_text, re.IGNORECASE)
    rain_prob_match = re.search(
        r'(\d{1,3})\s*%\s*(?:chance|probability)', page_text, re.IGNORECASE
    )
    rain_range_match = re.search(
        r'(\d+(?:\.\d+)?)\s*[-–to]+\s*(\d+(?:\.\d+)?)\s*mm', page_text, re.IGNORECASE
    )
    rain_single_match = re.search(r'(\d+(?:\.\d+)?)\s*mm', page_text, re.IGNORECASE)

    if not max_match:
        return None

    if rain_range_match:
        rain_min_v = safe_float(rain_range_match.group(1), 0.0)
        rain_max_v = safe_float(rain_range_match.group(2), 0.0)
    elif rain_single_match:
        rain_min_v = rain_max_v = safe_float(rain_single_match.group(1), 0.0)
    else:
        rain_min_v = rain_max_v = 0.0

    max_t = safe_float(max_match.group(1))
    log.info(f"WZ text regex: Max {max_t}°C")
    return {
        'Min_Temp': safe_float(min_match.group(1)) if min_match else None,
        'Max_Temp': max_t,
        'Rain_Prob': safe_float(rain_prob_match.group(1)) if rain_prob_match else None,
        'Rain_Min': rain_min_v,
        'Rain_Max': rain_max_v,
    }


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

# All 3 stations share the same Weatherzone Adelaide URL.
# Scrape once and reuse so Playwright only launches one browser session.
_wz_cache: dict[str, dict | None] = {}

def _get_wz(url: str) -> dict | None:
    if url not in _wz_cache:
        _wz_cache[url] = scrape_weatherzone(url)
    return _wz_cache[url]


for name, cfg in STATIONS.items():

    # --- Open-Meteo ---
    if already_exists(name, 'Open-Meteo'):
        log.info(f"  Open-Meteo | {name}: already recorded, skipping")
    else:
        om = fetch_open_meteo(cfg['lat'], cfg['lon'])
        if om and validate_forecast(om, 'Open-Meteo', name):
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
        if bom_fc and validate_forecast(bom_fc, 'BOM', name):
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
        wz = _get_wz(cfg['wz_url'])
        if wz and validate_forecast(wz, 'Weatherzone', name):
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
