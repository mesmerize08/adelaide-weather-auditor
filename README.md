# 🌤️ Adelaide Weather Accuracy Auditor

An automated, 100% free data pipeline and interactive dashboard designed to track, grade, and visualize the accuracy of various weather forecasting models for Adelaide, South Australia. 

**Live Dashboard:** [Insert your Streamlit App URL here]



## 🎯 Project Overview
Weather forecasts are everywhere, but how often are they actually correct? This project captures daily 9:00 AM forecasts from multiple leading weather models and compares them to the official Bureau of Meteorology (BOM) actuals recorded 24 hours later. 

By calculating the Mean Absolute Error (MAE) for both temperature and rainfall, this tool creates a rolling 30-day leaderboard to determine which weather source is truly the most reliable for Adelaide.

## ⚙️ How It Works (The Pipeline)
This project operates on a completely serverless, zero-cost architecture:
* **The Orchestrator:** GitHub Actions runs a cron job daily to trigger the data pipeline.
* **The "9 AM Rule":** To ensure fairness, the Python script strictly enforces an execution time of 9:00 AM Adelaide Time (handling ACST/ACDT daylight saving shifts automatically).
* **The Fetcher:** `weather_fetcher.py` queries free APIs and scrapes web sources for the day's forecasts, while simultaneously grabbing the previous day's *actual* recorded observations.
* **The Database:** Data is appended to a flat `weather_history.csv` file, which is automatically committed back to the GitHub repository.
* **The Frontend:** Streamlit Community Cloud continuously monitors the repository and updates the live dashboard whenever the CSV is modified.

## 📡 Data Sources
This project relies entirely on public, key-free data sources:
* **Open-Meteo (ECMWF):** The European model.
* **Open-Meteo (GFS):** The American model.
* **Open-Meteo (BOM ACCESS):** The Australian global model.
* **Weatherzone:** Web-scraped general Adelaide forecast.
* **BOM JSON Feed:** Used exclusively to verify "Actuals" (Min/Max Temp and Rainfall mm) for the grading phase.

## 📍 Monitored Stations
* Adelaide West Terrace
* Adelaide Airport
* Kent Town
* Mount Lofty

## 🚀 Features
* **30-Day Accuracy Leaderboard:** Ranks weather models by lowest average error.
* **Interactive Trend Charts:** Visualizes temperature error margins over time.
* **Daily Breakdown:** A calendar picker to review specific days with calculated delta metrics (e.g., predicted 25°C, actual 27°C -> -2.0°C error).
* **The "Perfect Day" Filter:** Isolates instances where a model perfectly predicted the temperature (within 1.0°C) and the exact rainfall range.

## 🛠️ Tech Stack
* **Language:** Python 3.10
* **Data Processing:** Pandas, Numpy
* **Web Scraping / Requests:** BeautifulSoup4, Requests
* **Automation:** GitHub Actions
* **Frontend:** Streamlit

## 💻 Local Setup
If you want to clone this repository and run it locally:

1. Clone the repo:
   ```bash
   git clone [https://github.com/your-username/adelaide-weather-auditor.git](https://github.com/your-username/adelaide-weather-auditor.git)
