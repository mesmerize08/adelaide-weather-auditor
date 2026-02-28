import streamlit as st
import pandas as pd
import numpy as np

# Page Config must be the first Streamlit command
st.set_page_config(page_title="Adelaide Weather Auditor", page_icon="🌤️", layout="wide")

@st.cache_data(ttl=3600)
def load_data():
    try:
        df = pd.read_csv('weather_history.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    except FileNotFoundError:
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data found. The GitHub Action may need to run its first cycle.")
    st.stop()

# --- 4. Data Completeness Warnings ---
latest_date = df['Date'].max()
missing_actuals = df[df['Date'] == latest_date]['Actual_Max_Temp'].isna().sum()
if missing_actuals > 0:
    st.warning(f"⚠️ Waiting on final BOM actuals for {latest_date.strftime('%Y-%m-%d')} before grading yesterday's forecasts.")

# Prepare Evaluation Data
df_eval = df.dropna(subset=['Actual_Min_Temp', 'Actual_Max_Temp']).copy()
if df_eval.empty:
    st.info("No completed actuals available yet to calculate accuracy.")
    st.stop()

# Core Error Calculations
df_eval['Temp_Error'] = abs(df_eval['Forecast_Max_Temp'] - df_eval['Actual_Max_Temp'])
df_eval['Forecast_Rain_Mid'] = (df_eval['Forecast_Rain_Min_mm'] + df_eval['Forecast_Rain_Max_mm']) / 2
df_eval['Rain_Error'] = abs(df_eval['Forecast_Rain_Mid'] - df_eval['Actual_Rain_mm'])
# Define a "Perfect" Rain hit (Actual falls within the predicted min/max range)
df_eval['Rain_Success'] = (df_eval['Actual_Rain_mm'] >= df_eval['Forecast_Rain_Min_mm']) & (df_eval['Actual_Rain_mm'] <= df_eval['Forecast_Rain_Max_mm'])

# --- 5. Sidebar Navigation ---
st.sidebar.title("⚙️ Dashboard Controls")
selected_station = st.sidebar.selectbox("📍 Select Station", df['Station'].unique())
selected_date = st.sidebar.date_input("📅 Select Date", value=df_eval['Date'].max().date())

st.sidebar.divider()

# --- 6. The "Perfect Day" Filter ---
st.sidebar.header("🌟 Filters")
st.sidebar.write("A **Perfect Day** means the temperature was predicted within 1.0°C and the rainfall was 100% correct.")
show_perfect_only = st.sidebar.checkbox("Show 'Perfect Days' Only")

if show_perfect_only:
    # Filter dataset down to only highly accurate predictions
    df_eval = df_eval[(df_eval['Temp_Error'] <= 1.0) & (df_eval['Rain_Success'])]
    st.sidebar.success(f"Found {len(df_eval)} Perfect Forecasts for this station!")

# --- MAIN DASHBOARD ---
st.title("🌤️ Adelaide Weather Accuracy Auditor")

# --- Section 1: Leaderboard & 2. Trend Charts ---
st.header(f"🏆 30-Day Accuracy for {selected_station}")
recent_30 = df_eval[df_eval['Date'] >= (pd.Timestamp.now() - pd.Timedelta(days=30))]
station_30 = recent_30[recent_30['Station'] == selected_station]

if not station_30.empty:
    # Leaderboard Table
    leaderboard = station_30.groupby('Source').agg(
        Temp_MAE=('Temp_Error', 'mean'),
        Rain_MAE=('Rain_Error', 'mean')
    ).reset_index().round(2)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Temperature Accuracy (Avg Error °C)")
        st.dataframe(leaderboard.sort_values('Temp_MAE')[['Source', 'Temp_MAE']], hide_index=True, use_container_width=True)
    with col2:
        st.subheader("Rainfall Accuracy (Avg Error mm)")
        st.dataframe(leaderboard.sort_values('Rain_MAE')[['Source', 'Rain_MAE']], hide_index=True, use_container_width=True)
        
    # Interactive Trend Chart
    st.subheader("📉 Temperature Error Trend (Last 30 Days)")
    st.write("Lower lines mean better accuracy.")
    # Pivot data so each source is a line on the chart
    chart_data = station_30.pivot_table(index='Date', columns='Source', values='Temp_Error')
    st.line_chart(chart_data)
else:
    st.info("No data matches the current filters for the last 30 days.")

st.divider()

# --- Section 2: Daily Performance & 1. Metrics with Deltas ---
st.header(f"📅 Daily Breakdown: {selected_date}")
day_data = df_eval[(df_eval['Date'].dt.date == selected_date) & (df_eval['Station'] == selected_station)]

if not day_data.empty:
    actual_max = day_data['Actual_Max_Temp'].iloc[0]
    actual_rain = day_data['Actual_Rain_mm'].iloc[0]
    
    st.markdown(f"**Official BOM Actuals:** Max Temp: `{actual_max}°C` | Rain: `{actual_rain}mm`")
    
    # Create dynamic columns based on how many sources we have
    cols = st.columns(len(day_data))
    
    for i, (idx, row) in enumerate(day_data.iterrows()):
        source = row['Source']
        f_max = row['Forecast_Max_Temp']
        r_min, r_max = row['Forecast_Rain_Min_mm'], row['Forecast_Rain_Max_mm']
        rain_success = row['Rain_Success']
        
        # Calculate Delta (Forecast minus Actual)
        temp_diff = f_max - actual_max
        rain_status = "✅ Correct" if rain_success else "❌ Incorrect"
        
        with cols[i]:
            st.markdown(f"**{source}**")
            # Metrics with Delta. "inverse" means positive numbers (over-forecasting) turn red.
            st.metric(
                label="Predicted Max Temp", 
                value=f"{f_max}°C", 
                delta=f"{temp_diff:+.1f}°C vs Actual", 
                delta_color="inverse"
            )
            st.metric(
                label="Predicted Rain", 
                value=f"{r_min}-{r_max} mm", 
                delta=rain_status, 
                delta_color="off"
            )
else:
    st.info("No performance data available for this specific date, station, and filter combination.")
