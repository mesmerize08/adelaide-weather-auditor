import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Adelaide Weather Auditor", layout="wide")

@st.cache_data(ttl=3600)
def load_data():
    try:
        df = pd.read_csv('weather_history.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    except FileNotFoundError:
        return pd.DataFrame()

df = load_data()

st.title("🌤️ Adelaide Weather Accuracy Auditor")

if df.empty:
    st.warning("No data found. The GitHub Action may need to run its first cycle.")
    st.stop()

# --- Calculations for MAE ---
# Drop rows where actuals haven't been populated yet
df_eval = df.dropna(subset=['Actual_Min_Temp', 'Actual_Max_Temp']).copy()

df_eval['Temp_Error'] = abs(df_eval['Forecast_Max_Temp'] - df_eval['Actual_Max_Temp'])
# Calculate mid-point of rain range for error checking
df_eval['Forecast_Rain_Mid'] = (df_eval['Forecast_Rain_Min_mm'] + df_eval['Forecast_Rain_Max_mm']) / 2
df_eval['Rain_Error'] = abs(df_eval['Forecast_Rain_Mid'] - df_eval['Actual_Rain_mm'])

# --- Section 1: Leaderboard ---
st.header("🏆 30-Day Accuracy Leaderboard")
st.write("Ranked by lowest Mean Absolute Error (MAE)")

recent_30 = df_eval[df_eval['Date'] >= (pd.Timestamp.now() - pd.Timedelta(days=30))]

if not recent_30.empty:
    leaderboard = recent_30.groupby('Source').agg(
        Temp_MAE=('Temp_Error', 'mean'),
        Rain_MAE=('Rain_Error', 'mean')
    ).reset_index().round(2)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Temperature Accuracy")
        st.dataframe(leaderboard.sort_values('Temp_MAE')[['Source', 'Temp_MAE']], hide_index=True, use_container_width=True)
    with col2:
        st.subheader("Rainfall Accuracy")
        st.dataframe(leaderboard.sort_values('Rain_MAE')[['Source', 'Rain_MAE']], hide_index=True, use_container_width=True)
else:
    st.info("Insufficient historical data for a 30-day leaderboard. Check back tomorrow!")

st.divider()

# --- Section 2: Calendar View ---
st.header("📅 Daily Forecast vs. Actuals")

col3, col4 = st.columns(2)
with col3:
    selected_date = st.date_input("Select Date", value=df_eval['Date'].max() if not df_eval.empty else pd.Timestamp.now())
with col4:
    selected_station = st.selectbox("Select Station", df['Station'].unique())

day_data = df_eval[(df_eval['Date'].dt.date == selected_date) & (df_eval['Station'] == selected_station)]

if not day_data.empty:
    actual_max = day_data['Actual_Max_Temp'].iloc[0]
    actual_rain = day_data['Actual_Rain_mm'].iloc[0]
    
    st.markdown(f"**Official BOM Actuals for {selected_date}:** Max Temp: {actual_max}°C | Rain: {actual_rain}mm")
    
    for _, row in day_data.iterrows():
        source = row['Source']
        f_max = row['Forecast_Max_Temp']
        r_min, r_max = row['Forecast_Rain_Min_mm'], row['Forecast_Rain_Max_mm']
        
        # Determine Rain Success String
        if r_min <= actual_rain <= r_max:
            rain_status = "✅ SUCCESS"
        else:
            rain_status = "❌ FAILED"
            
        with st.expander(f"{source} Performance"):
            st.write(f"**Temperature:** Predicted {f_max}°C (Error: {abs(f_max - actual_max):.1f}°C)")
            st.write(f"**Rainfall:** Predicted {r_min}-{r_max}mm. Actual was {actual_rain}mm - {rain_status}")
else:
    st.info("No audit data available for this specific date/station combination.")
