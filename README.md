# 🌤️ Adelaide Weather Accuracy Auditor

An automated, 100% free data pipeline and interactive dashboard that tracks, grades, and visualises the accuracy of weather forecasts for Adelaide, South Australia.

**Live Dashboard:** [adelaide-weather-auditor.streamlit.app](https://adelaide-weather-auditor.streamlit.app/)

## 🎯 Project Overview

Weather forecasts are everywhere, but how often are they actually correct? This project captures daily 9:00 AM forecasts from three competing weather platforms and compares them to the official Bureau of Meteorology actuals recorded 24 hours later.

By calculating the Mean Absolute Error (MAE) for temperature and rainfall, this tool creates a rolling 30-day leaderboard to determine which source is truly the most reliable for Adelaide.

## ⚙️ How It Works

This project runs on a completely serverless, zero-cost architecture:

1. **GitHub Actions** triggers the pipeline daily via cron.
2. **The 9 AM Rule** — the Python script enforces an 8:30–10:00 AM Adelaide time execution window (handling ACST/ACDT daylight saving automatically via dual cron triggers).
3. **`weather_fetcher.py`** captures today's forecasts from all three sources, and simultaneously fetches yesterday's actual recorded observations from BOM.
4. **`weather_history.csv`** stores all data and is auto-committed back to the repo.
5. **Streamlit Community Cloud** monitors the repo and updates the live dashboard whenever the CSV changes.

## 📡 Data Sources

### Forecast Sources (Predictions — captured at 9 AM)
| Source | Method | Location-Specific? |
|--------|--------|--------------------|
| **BOM** | Scraped from bom.gov.au/places/ — the human-curated forecast BOM meteorologists publish | ✅ Per station |
| **Open-Meteo** | API (Best Match mode — auto-selects the optimal model for Adelaide) | ✅ Per lat/lon |
| **Weatherzone** | Scraped from weatherzone.com.au — general Adelaide forecast | ❌ Adelaide-wide |

### Actuals Source (Grading — collected next day)
* **BOM Observation JSON Feed** — official recorded observations (Min/Max Temp and Rainfall mm) from BOM weather station instruments.

## 📍 Monitored Stations
* Adelaide West Terrace (BOM 94648)
* Adelaide Airport (BOM 94146)
* Mount Lofty (BOM 95678)
* Noarlunga (BOM 94808)

## 🚀 Features
* **30-Day Accuracy Leaderboard** — ranks BOM, Open-Meteo, and Weatherzone by lowest average error for both max and min temperature.
* **Interactive Trend Charts** — visualises temperature error margins over time.
* **Daily Breakdown** — pick any date to see each source's prediction vs actual, with delta metrics.
* **"Perfect Day" Filter** — isolates forecasts where max temp was within 1.0°C and rainfall fell within the predicted range.
* **Data Coverage Summary** — shows which sources are reporting and where actuals are still pending.

## 🛠️ Tech Stack
* **Language:** Python 3.10
* **Data Processing:** Pandas, Numpy
* **Web Scraping:** BeautifulSoup4, Requests
* **Automation:** GitHub Actions
* **Frontend:** Streamlit

## 💻 Local Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/adelaide-weather-auditor.git
   cd adelaide-weather-auditor
   ```
2. Install dependencies:
   ```bash
   pip install requests beautifulsoup4 pandas pytz streamlit numpy
   ```
3. Run the fetcher manually (respects the 9 AM time window):
   ```bash
   python weather_fetcher.py
   ```
4. Launch the dashboard:
   ```bash
   streamlit run streamlit_app.py
   ```

## 📋 Known Limitations
* **Weatherzone scraping** is inherently fragile — if they change their HTML layout, the scraper may need selector updates.
* **Open-Meteo rain forecasts** are a single value (`precipitation_sum`), not a min/max range. The rain range only has real width for BOM and Weatherzone data.
* **BOM forecast pages** occasionally omit the Min temp for "rest of today" forecasts issued after the morning minimum has already passed.
