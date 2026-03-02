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

# --- Data Completeness Warnings ---
latest_date = df['Date'].max()
missing_actuals = df[df['Date'] == latest_date]['Actual_Max_Temp'].isna().sum()
total_latest = len(df[df['Date'] == latest_date])
if missing_actuals > 0 and missing_actuals == total_latest:
    st.warning(f"⚠️ Waiting on BOM actuals for {latest_date.strftime('%Y-%m-%d')}. Grading will update after tomorrow's run.")

# --- Data Quality: count how many sources are reporting ---
sources_today = df[df['Date'] == latest_date]['Source'].nunique()
expected_sources = 4  # ECMWF, GFS, BOM, Weatherzone
if sources_today < expected_sources:
    active_sources = df[df['Date'] == latest_date]['Source'].unique().tolist()
    st.info(f"📡 Active sources today: {', '.join(active_sources)} ({sources_today}/{expected_sources})")

# Prepare Evaluation Data — only rows where actuals exist
df_eval = df.dropna(subset=['Actual_Min_Temp', 'Actual_Max_Temp']).copy()

# Also require forecast data to exist for meaningful comparison
df_eval = df_eval.dropna(subset=['Forecast_Max_Temp']).copy()

if df_eval.empty:
    st.info("No completed forecast+actuals pairs available yet. The pipeline needs at least 2 days of data to start grading.")
    st.stop()

# --- Core Error Calculations ---
df_eval['Max_Temp_Error'] = abs(df_eval['Forecast_Max_Temp'] - df_eval['Actual_Max_Temp'])
df_eval['Min_Temp_Error'] = abs(df_eval['Forecast_Min_Temp'] - df_eval['Actual_Min_Temp'])

# Rain: midpoint of forecast range vs actual
df_eval['Forecast_Rain_Mid'] = (
    df_eval['Forecast_Rain_Min_mm'].fillna(0) + df_eval['Forecast_Rain_Max_mm'].fillna(0)
) / 2
df_eval['Rain_Error'] = abs(df_eval['Forecast_Rain_Mid'] - df_eval['Actual_Rain_mm'].fillna(0))

# "Perfect" Rain = actual falls within the predicted min/max range
df_eval['Rain_Success'] = (
    (df_eval['Actual_Rain_mm'] >= df_eval['Forecast_Rain_Min_mm']) &
    (df_eval['Actual_Rain_mm'] <= df_eval['Forecast_Rain_Max_mm'])
)

# --- Sidebar Navigation ---
st.sidebar.title("⚙️ Dashboard Controls")
selected_station = st.sidebar.selectbox("📍 Select Station", sorted(df['Station'].unique()))

available_dates = sorted(df_eval['Date'].dt.date.unique(), reverse=True)
if available_dates:
    selected_date = st.sidebar.date_input(
        "📅 Select Date",
        value=available_dates[0],
        min_value=available_dates[-1],
        max_value=available_dates[0],
    )
else:
    selected_date = st.sidebar.date_input("📅 Select Date")

st.sidebar.divider()

# --- The "Perfect Day" Filter ---
st.sidebar.header("🌟 Filters")
st.sidebar.write("A **Perfect Day** means the max temp was predicted within 1.0°C and the actual rainfall fell within the forecast range.")
show_perfect_only = st.sidebar.checkbox("Show 'Perfect Days' Only")

# Apply filter on a COPY to avoid mutating df_eval for other sections
df_display = df_eval.copy()
if show_perfect_only:
    df_display = df_display[(df_display['Max_Temp_Error'] <= 1.0) & (df_display['Rain_Success'])]
    st.sidebar.success(f"Found {len(df_display)} perfect forecasts across all stations!")

# --- MAIN DASHBOARD ---
st.title("🌤️ Adelaide Weather Accuracy Auditor")
st.caption("Comparing forecast accuracy across weather models for Adelaide, SA. Data sourced from Open-Meteo, Weatherzone, and BOM.")

# --- Section 1: Leaderboard ---
st.header(f"🏆 30-Day Accuracy: {selected_station}")

cutoff_30 = pd.Timestamp.now() - pd.Timedelta(days=30)
station_30 = df_display[
    (df_display['Date'] >= cutoff_30) &
    (df_display['Station'] == selected_station)
]

if not station_30.empty:
    # Leaderboard Table
    leaderboard = station_30.groupby('Source').agg(
        Max_Temp_MAE=('Max_Temp_Error', 'mean'),
        Min_Temp_MAE=('Min_Temp_Error', 'mean'),
        Rain_MAE=('Rain_Error', 'mean'),
        Rain_Hit_Rate=('Rain_Success', 'mean'),
        Forecasts=('Source', 'count'),
    ).reset_index().round(2)

    # Convert rain hit rate to percentage
    leaderboard['Rain_Hit_Rate'] = (leaderboard['Rain_Hit_Rate'] * 100).round(1).astype(str) + '%'

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🌡️ Temperature Accuracy")
        temp_lb = leaderboard.sort_values('Max_Temp_MAE')[
            ['Source', 'Max_Temp_MAE', 'Min_Temp_MAE', 'Forecasts']
        ].rename(columns={
            'Max_Temp_MAE': 'Max Temp MAE (°C)',
            'Min_Temp_MAE': 'Min Temp MAE (°C)',
            'Forecasts': '# Days',
        })
        st.dataframe(temp_lb, hide_index=True, use_container_width=True)

    with col2:
        st.subheader("🌧️ Rainfall Accuracy")
        rain_lb = leaderboard.sort_values('Rain_MAE')[
            ['Source', 'Rain_MAE', 'Rain_Hit_Rate', 'Forecasts']
        ].rename(columns={
            'Rain_MAE': 'Rain MAE (mm)',
            'Rain_Hit_Rate': 'In-Range %',
            'Forecasts': '# Days',
        })
        st.dataframe(rain_lb, hide_index=True, use_container_width=True)

    # Interactive Trend Chart
    st.subheader("📉 Max Temp Error Trend (Last 30 Days)")
    st.caption("Lower = more accurate.")
    chart_data = station_30.pivot_table(index='Date', columns='Source', values='Max_Temp_Error')
    st.line_chart(chart_data)
else:
    st.info("No graded data for this station in the last 30 days. Keep the pipeline running!")

st.divider()

# --- Section 2: Daily Breakdown ---
st.header(f"📅 Daily Breakdown: {selected_date}")
day_data = df_display[
    (df_display['Date'].dt.date == selected_date) &
    (df_display['Station'] == selected_station)
]

if not day_data.empty:
    actual_max = day_data['Actual_Max_Temp'].iloc[0]
    actual_min = day_data['Actual_Min_Temp'].iloc[0]
    actual_rain = day_data['Actual_Rain_mm'].iloc[0]

    st.markdown(
        f"**BOM Actuals:** Max: `{actual_max}°C` · Min: `{actual_min}°C` · Rain: `{actual_rain}mm`"
    )

    # Create columns per source
    cols = st.columns(len(day_data))

    for i, (idx, row) in enumerate(day_data.iterrows()):
        source = row['Source']
        f_max = row['Forecast_Max_Temp']
        f_min = row['Forecast_Min_Temp']
        r_min = row['Forecast_Rain_Min_mm']
        r_max = row['Forecast_Rain_Max_mm']
        rain_success = row['Rain_Success']

        temp_diff = f_max - actual_max
        rain_status = "✅ In Range" if rain_success else "❌ Missed"

        with cols[i]:
            st.markdown(f"**{source}**")
            st.metric(
                label="Predicted Max",
                value=f"{f_max}°C",
                delta=f"{temp_diff:+.1f}°C error",
                delta_color="inverse"
            )
            if pd.notna(f_min):
                min_diff = f_min - actual_min
                st.metric(
                    label="Predicted Min",
                    value=f"{f_min}°C",
                    delta=f"{min_diff:+.1f}°C error",
                    delta_color="inverse"
                )
            st.metric(
                label="Predicted Rain",
                value=f"{r_min:.0f}–{r_max:.0f} mm",
                delta=rain_status,
                delta_color="off"
            )
else:
    st.info("No graded data for this date/station/filter combination.")

st.divider()

# --- Section 3: Data Coverage ---
with st.expander("📊 Data Coverage Summary"):
    coverage = df.groupby(['Date', 'Station']).agg(
        Sources=('Source', 'nunique'),
        Has_Actuals=('Actual_Max_Temp', lambda x: x.notna().any()),
        Has_Forecasts=('Forecast_Max_Temp', lambda x: x.notna().any()),
    ).reset_index()
    coverage['Date'] = coverage['Date'].dt.strftime('%Y-%m-%d')
    st.dataframe(coverage.sort_values('Date', ascending=False).head(30), hide_index=True, use_container_width=True)

# --- Footer ---
st.divider()
st.caption("Data: Open-Meteo (ECMWF, GFS, BOM ACCESS), Weatherzone, Bureau of Meteorology · Updated daily at ~9 AM ACST")
