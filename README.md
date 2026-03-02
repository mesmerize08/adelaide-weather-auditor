# 🌤️ Adelaide Weather Accuracy Auditor

An automated, 100% free data pipeline and interactive dashboard designed to track, grade, and visualize the accuracy of various weather forecasting models for Adelaide, South Australia. 

**Live Dashboard:** [adelaide-weather-auditor.streamlit.app] https://adelaide-weather-auditor.streamlit.app/



## 🎯 Project Overview
Weather forecasts are everywhere, but how often are they actually correct? This project captures daily 9:00 AM forecasts from multiple leading weather models and compares them to the official Bureau of Meteorology (BOM) actuals recorded 24 hours later. 

By calculating the Mean Absolute Error (MAE) for both temperature and rainfall, this tool creates a rolling 30-day leaderboard to determine which weather source is truly the most reliable for Adelaide.

## ⚙️ How It Works (The Pipeline)
This project operates on a completely serverless, zero-cost architecture:
* **The Orchestrator:** GitHub Actions runs a cron job daily to trigger the data pipeline.
* **The "9 AM Rule":** The Python script enforces an execution window of 8:30–10:00 AM Adelaide Time (handling ACST/ACDT daylight saving shifts automatically via dual cron triggers).
* **The Fetcher:** `weather_fetcher.py` queries free APIs and scrapes web sources for the day's forecasts, while simultaneously grabbing the previous day's *actual* recorded observations.
* **The Database:** Data is appended to a flat `weather_history.csv` file, which is automatically committed back to the GitHub repository.
* **The Frontend:** Streamlit Community Cloud continuously monitors the repository and updates the live dashboard whenever the CSV is modified.

## 📡 Data Sources
This project relies entirely on public, key-free data sources:
* **Open-Meteo (ECMWF IFS 0.25°):** The European model.
* **Open-Meteo (GFS Seamless):** The American model.
* **Open-Meteo (BOM ACCESS Global):** The Australian global model (currently suspended by BOM — will auto-recover when BOM resumes open-data delivery).
* **Weatherzone:** Web-scraped general Adelaide forecast.
* **BOM JSON Feed:** Used exclusively to verify "Actuals" (Min/Max Temp and Rainfall mm) for the grading phase.

## 📍 Monitored Stations
* Adelaide West Terrace (BOM 94648)
* Adelaide Airport (BOM 94146)
* Mount Lofty (BOM 95678)
* Noarlunga (BOM 94808)

## 🚀 Features
* **30-Day Accuracy Leaderboard:** Ranks weather models by lowest average error for both max and min temperature.
* **Interactive Trend Charts:** Visualises temperature error margins over time.
* **Daily Breakdown:** A calendar picker to review specific days with calculated delta metrics (e.g., predicted 25°C, actual 27°C → -2.0°C error).
* **The "Perfect Day" Filter:** Isolates instances where a model predicted max temp within 1.0°C and rainfall within the forecast range.
* **Data Coverage Summary:** Shows which sources are reporting and where actuals are still pending.

## 🛠️ Tech Stack
* **Language:** Python 3.10
* **Data Processing:** Pandas, Numpy
* **Web Scraping / Requests:** BeautifulSoup4, Requests
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
* **BOM ACCESS on Open-Meteo** is currently unavailable due to BOM suspending open-data delivery. The pipeline handles this gracefully and will resume automatically.
* **Weatherzone scraping** is inherently fragile. If Weatherzone changes their HTML layout, the scraper may need selector updates.
* **Open-Meteo rain forecasts** are single-value (`precipitation_sum`), not a range. The rain min/max range only has real width for Weatherzone data.
