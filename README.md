# Adelaide Weather Accuracy Auditor

A fully automated weather forecast accuracy tracker for Adelaide, South Australia. Every morning at 9 AM Adelaide time, forecasts are collected from three sources, yesterday's actuals are backfilled from BOM observations, and a Streamlit dashboard is updated — all without any manual intervention.

**Live app:** https://adelaide-weather-auditor.streamlit.app/
**Data store:** [`weather_history.csv`](weather_history.csv) — committed to this repo daily by GitHub Actions

---

## What it does

| Step | Time | Action |
|---|---|---|
| Collect forecasts | 9 AM daily | Fetches today's forecast from BOM, Weatherzone, Open-Meteo for all 3 stations |
| Backfill actuals | 9 AM daily | Updates yesterday's rows with real BOM observations (falls back to Open-Meteo) |
| Commit data | 9 AM daily | `weather_history.csv` pushed to `main` via `git-auto-commit-action` |
| Keep app alive | After commit | Pings Streamlit URL to prevent the 7-day inactivity sleep |

---

## Stations tracked

| Station | Lat/Lon | BOM WMO ID |
|---|---|---|
| West Terrace (CBD) | −34.9285, 138.5955 | 94648 |
| Adelaide Airport | −34.9524, 138.5196 | 94672 |
| Mt Lofty Summit | −34.9800, 138.7083 | 94693 |

---

## Data sources

### Forecasts
| Source | Method | Notes |
|---|---|---|
| **BOM** | `api.weather.bom.gov.au` JSON API | Geohash-based location lookup; Mt Lofty uses hardcoded geohash `r1fy9t` |
| **Weatherzone** | Playwright (headless Chromium) | React SPA behind Cloudflare — plain HTTP is blocked; Playwright renders the page and intercepts XHR JSON. All 3 stations share the Adelaide city URL so only one browser session is launched |
| **Open-Meteo** | Free REST API | No key required; `precipitation_sum` used as both rain min and max (single point estimate) |

### Actuals
- **Primary:** BOM IDS60901 observation JSON (`http://www.bom.gov.au/fwo/IDS60901/IDS60901.{WMO_ID}.json`) — 30-min interval readings, newest first; up to 48 readings (~24 h) used to compute daily min/max
- **Fallback:** Open-Meteo historical API (`start_date`/`end_date` for yesterday) if BOM observations are unreachable

---

## Dashboard tabs

| Tab | Contents |
|---|---|
| **📅 Calendar View** | Error heatmap (green→red by MAE), source disagreement indicator, per-day inspector with forecast vs actual cards |
| **🏆 Leaderboard** | Streak tracker, MAE ranking table + bar chart, detailed rain accuracy breakdown (hit rate / false alarm / miss rate) |
| **📈 Trend Analysis** | Absolute error over time, rainfall error over time, forecast vs actual scatter with perfect-prediction diagonal |
| **🗓️ Monthly Summary** | MAE pivot table by month × source, monthly bar chart, win tally per source |
| **📋 Raw Data** | Filterable table + CSV download |

**Live panel** at the top of every page shows real-time BOM observations (temp, feels like, humidity, wind, rain since 9am) for the selected station — auto-refreshed every 30 minutes.

---

## Accuracy metrics

| Metric | Definition |
|---|---|
| **MAE** | Mean Absolute Error of forecast vs actual (°C or mm) |
| **Rain Range Hit %** | % of days where actual rainfall fell within the forecast min–max range |
| **Hit Rate (rain days)** | On days it actually rained, % where the forecast range included the actual |
| **False Alarm Rate** | % of dry days where the source predicted rain (prob > 30%) |
| **Miss Rate** | % of rain days where the source did not predict rain |

---

## Repository structure

```
adelaide-weather-auditor/
├── weather_fetcher.py        # GitHub Actions data collector
├── streamlit_app.py          # Streamlit dashboard
├── weather_history.csv       # Live data store (auto-committed daily)
├── requirements.txt          # Streamlit Cloud dependencies
└── .github/
    └── workflows/
        └── weather_auditor.yml   # Cron + keep-alive workflow
```

---

## How it runs (GitHub Actions)

The workflow [`.github/workflows/weather_auditor.yml`](.github/workflows/weather_auditor.yml) runs on two cron schedules to cover both Adelaide daylight saving transitions:

- `30 22 * * *` UTC = **9:00 AM ACDT** (Oct–Apr, UTC+10:30)
- `30 23 * * *` UTC = **9:00 AM ACST** (Apr–Oct, UTC+9:30)

Both crons run every day. Deduplication in `weather_fetcher.py` ensures only the first run that falls closest to 9 AM produces new data — the second run is a no-op.

### Playwright caching

Playwright Chromium (~300 MB) is cached using `actions/cache@v4` keyed on OS + `requirements.txt` hash. On a cache hit, only the OS system libraries are re-installed (`playwright install-deps chromium`), saving ~90 seconds per run.

### No secrets required

- `GITHUB_TOKEN` is auto-provided by GitHub Actions
- `permissions: contents: write` grants the commit step write access
- All weather APIs used are free and require no API keys

---

## Local development

```bash
# Install dependencies
pip install -r requirements.txt
pip install playwright
playwright install chromium --with-deps

# Run the data fetcher (writes to weather_history.csv)
python weather_fetcher.py

# Run the Streamlit app (reads local CSV as fallback)
streamlit run streamlit_app.py
```

The app detects whether it's running locally (reads `weather_history.csv`) or on Streamlit Cloud (reads from the GitHub raw URL).

---

## Data schema

`weather_history.csv` columns:

| Column | Type | Description |
|---|---|---|
| `Date` | YYYY-MM-DD | Forecast date |
| `Station` | string | `West Terrace`, `Airport`, or `Mt Lofty` |
| `Source` | string | `BOM`, `Weatherzone`, or `Open-Meteo` |
| `Forecast_Min_Temp` | float | Forecast minimum temperature (°C) |
| `Forecast_Max_Temp` | float | Forecast maximum temperature (°C) |
| `Forecast_Rain_Prob` | float | Forecast chance of rain (%) |
| `Forecast_Rain_Min_mm` | float | Forecast rainfall lower bound (mm) |
| `Forecast_Rain_Max_mm` | float | Forecast rainfall upper bound (mm) |
| `Actual_Min_Temp` | float | Observed minimum temperature (°C) — backfilled next morning |
| `Actual_Max_Temp` | float | Observed maximum temperature (°C) — backfilled next morning |
| `Actual_Rain_mm` | float | Observed rainfall since 9am (mm) — backfilled next morning |

---

## Known limitations

- **Weatherzone is city-level only** — all three stations receive the same WZ Adelaide city forecast (WZ does not publish station-level forecasts)
- **Open-Meteo rain range** — Open-Meteo returns a single `precipitation_sum` value, so its rain min and max are identical; this means its "rain range hit rate" measures exact correctness rather than range coverage
- **BOM observations may be geo-blocked** from GitHub Actions IPs — Open-Meteo historical data is used as an automatic fallback
- **Mt Lofty BOM forecast** — BOM's location search does not return Mt Lofty (it is a summit, not a suburb); a hardcoded geohash (`r1fy9t`) is used directly
