"""
Adelaide Weather Accuracy Auditor — Streamlit Dashboard
Reads from GitHub raw CSV so it works on Streamlit Cloud.
"""

import io
import requests
import pytz
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────
# Page config (must be first Streamlit call)
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Adelaide Weather Auditor",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────
GITHUB_RAW_URL = (
    "https://raw.githubusercontent.com/"
    "mesmerize08/adelaide-weather-auditor/main/weather_history.csv"
)
LOCAL_CSV = "weather_history.csv"

STATIONS = ["West Terrace", "Airport", "Mt Lofty"]
SOURCE_COLORS = {
    "BOM": "#1f77b4",
    "Open-Meteo": "#2ca02c",
    "Weatherzone": "#d62728",
}

adelaide_tz = pytz.timezone("Australia/Adelaide")


# ─────────────────────────────────────────────────────────
# Data loading — reads from GitHub (works on Streamlit Cloud)
# Falls back to local file for development
# ─────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """Load weather history from GitHub, falling back to local CSV."""
    empty = pd.DataFrame(columns=[
        "Date", "Station", "Source",
        "Forecast_Min_Temp", "Forecast_Max_Temp",
        "Forecast_Rain_Prob", "Forecast_Rain_Min_mm", "Forecast_Rain_Max_mm",
        "Actual_Min_Temp", "Actual_Max_Temp", "Actual_Rain_mm",
    ])

    # 1. Try GitHub raw URL (production — Streamlit Cloud)
    try:
        resp = requests.get(GITHUB_RAW_URL, timeout=10)
        resp.raise_for_status()
        if resp.text.strip():
            df = pd.read_csv(io.StringIO(resp.text))
            source_label = "GitHub"
        else:
            return empty
    except Exception:
        # 2. Fallback: local file (development)
        try:
            df = pd.read_csv(LOCAL_CSV)
            source_label = "local CSV"
        except FileNotFoundError:
            return empty

    if df.empty:
        return empty

    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date", ascending=False).reset_index(drop=True)
    st.session_state["data_source"] = source_label
    return df


# ─────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────
now_adl = datetime.now(adelaide_tz)
st.title("🌤️ Adelaide Weather Accuracy Auditor")
st.caption(
    f"Adelaide time: **{now_adl.strftime('%A %d %B %Y, %I:%M %p %Z')}**  "
    f"· Forecasts updated daily at 9 AM · Data from BOM, Weatherzone, Open-Meteo"
)

# ─────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────
df = load_data()
data_source = st.session_state.get("data_source", "unknown")

if df.empty:
    st.warning("⏳ No data yet — waiting for the first automated collection run.")
    st.info(
        "The GitHub Actions workflow runs at **9 AM Adelaide time** daily.  \n"
        "Check back after the first run, or trigger it manually from the Actions tab."
    )
    st.stop()

# ─────────────────────────────────────────────────────────
# Sidebar — Filters
# ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Filters")

    selected_station = st.selectbox("Station", STATIONS)

    available_sources = sorted(df["Source"].dropna().unique().tolist())
    selected_sources = st.multiselect(
        "Sources", available_sources, default=available_sources
    )

    days_window = st.slider("Leaderboard window (days)", 7, 90, 30, step=7)

    st.divider()

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown(f"**Data source:** `{data_source}`")
    first_date = df["Date"].min()
    last_date = df["Date"].max()
    st.markdown(f"**Range:** `{first_date.strftime('%d %b %Y')}` → `{last_date.strftime('%d %b %Y')}`")
    st.markdown(f"**Total rows:** `{len(df):,}`")

    st.divider()
    st.markdown(
        "📦 [GitHub Repo](https://github.com/mesmerize08/adelaide-weather-auditor)  \n"
        "🚀 [Streamlit App](https://adelaide-weather-auditor.streamlit.app/)"
    )

# ─────────────────────────────────────────────────────────
# Derived data — filtered for current station + source selection
# ─────────────────────────────────────────────────────────
df_station = df[
    (df["Station"] == selected_station)
    & (df["Source"].isin(selected_sources))
].copy()

df_eval = df_station.dropna(subset=["Actual_Max_Temp"]).copy()
if not df_eval.empty:
    df_eval["Temp_Error"] = abs(df_eval["Forecast_Max_Temp"] - df_eval["Actual_Max_Temp"])
    df_eval["Temp_Min_Error"] = abs(df_eval["Forecast_Min_Temp"] - df_eval["Actual_Min_Temp"])
    df_eval["Forecast_Rain_Mid"] = (
        df_eval["Forecast_Rain_Min_mm"] + df_eval["Forecast_Rain_Max_mm"]
    ) / 2
    df_eval["Rain_Error"] = abs(df_eval["Forecast_Rain_Mid"] - df_eval["Actual_Rain_mm"])
    df_eval["Rain_Hit"] = (
        (df_eval["Forecast_Rain_Min_mm"] <= df_eval["Actual_Rain_mm"])
        & (df_eval["Actual_Rain_mm"] <= df_eval["Forecast_Rain_Max_mm"])
    )

# ─────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────
tab_cal, tab_leader, tab_trend, tab_raw = st.tabs([
    "📅 Calendar View",
    "🏆 Leaderboard",
    "📈 Trend Analysis",
    "📋 Raw Data",
])


# ══════════════════════════════════════════════════════════
# TAB 1 — Calendar / Day Inspector
# ══════════════════════════════════════════════════════════
with tab_cal:
    available_dates = sorted(df["Date"].dt.date.unique(), reverse=True)
    default_date = available_dates[0] if available_dates else now_adl.date()

    col_pick, col_detail = st.columns([1, 3])

    with col_pick:
        selected_date = st.date_input(
            "Select date",
            value=default_date,
            min_value=available_dates[-1] if len(available_dates) > 1 else None,
            max_value=available_dates[0] if available_dates else None,
        )
        st.caption(f"{len(available_dates)} days of data")

        # Quick navigation
        st.markdown("**Quick nav:**")
        nav_dates = available_dates[:7]
        for d in nav_dates:
            label = "Today" if d == now_adl.date() else d.strftime("%d %b")
            if st.button(label, key=f"nav_{d}", use_container_width=True):
                selected_date = d

    with col_detail:
        # Pull all sources for that date across all stations if station filter is relaxed
        day_all = df[df["Date"].dt.date == selected_date]
        day_data = day_all[
            (day_all["Station"] == selected_station)
            & (day_all["Source"].isin(selected_sources))
        ].copy()

        if day_data.empty:
            st.info(
                f"No data for **{selected_date}** / **{selected_station}**.  \n"
                "Data is collected at 9 AM daily — dates before the first run won't appear."
            )
        else:
            has_actuals = day_data["Actual_Max_Temp"].notna().any()

            if has_actuals:
                # Use first non-null actuals row (all sources share the same BOM actuals)
                actuals_row = day_data.dropna(subset=["Actual_Max_Temp"]).iloc[0]
                actual_max = actuals_row["Actual_Max_Temp"]
                actual_min = actuals_row["Actual_Min_Temp"]
                actual_rain = actuals_row["Actual_Rain_mm"]

                st.success(f"**BOM Observed — {selected_station}, {selected_date}**")
                c1, c2, c3 = st.columns(3)
                c1.metric("Max Temp", f"{actual_max:.1f} °C")
                c2.metric("Min Temp", f"{actual_min:.1f} °C" if pd.notna(actual_min) else "—")
                c3.metric("Rainfall", f"{actual_rain:.1f} mm" if pd.notna(actual_rain) else "—")
            else:
                st.info(
                    f"Actuals for **{selected_date}** haven't been recorded yet.  \n"
                    "They're backfilled the following morning at 9 AM."
                )

            st.divider()

            # Per-source forecast cards
            for _, row in day_data.iterrows():
                source = row["Source"]
                f_max = row.get("Forecast_Max_Temp")
                f_min = row.get("Forecast_Min_Temp")
                r_min = row.get("Forecast_Rain_Min_mm", 0) or 0
                r_max = row.get("Forecast_Rain_Max_mm", 0) or 0
                r_prob = row.get("Forecast_Rain_Prob")

                color = SOURCE_COLORS.get(source, "#888888")

                with st.expander(f"**{source}**", expanded=True):
                    ec1, ec2, ec3 = st.columns(3)

                    # Max temp
                    if has_actuals and pd.notna(f_max):
                        err = f_max - actual_max
                        ec1.metric(
                            "Forecast Max", f"{f_max:.1f} °C",
                            f"{err:+.1f} °C",
                            delta_color="inverse",
                        )
                    else:
                        ec1.metric("Forecast Max", f"{f_max:.1f} °C" if pd.notna(f_max) else "—")

                    # Min temp
                    if has_actuals and pd.notna(f_min) and pd.notna(actual_min):
                        err_min = f_min - actual_min
                        ec2.metric(
                            "Forecast Min", f"{f_min:.1f} °C",
                            f"{err_min:+.1f} °C",
                            delta_color="inverse",
                        )
                    else:
                        ec2.metric("Forecast Min", f"{f_min:.1f} °C" if pd.notna(f_min) else "—")

                    # Rain
                    rain_label = f"{r_min:.0f}–{r_max:.0f} mm"
                    if pd.notna(r_prob):
                        rain_label += f"  ({r_prob:.0f}%)"
                    if has_actuals and pd.notna(actual_rain):
                        rain_ok = r_min <= actual_rain <= r_max
                        hit_str = "✅ Hit" if rain_ok else "❌ Miss"
                        ec3.metric("Rain Forecast", rain_label, hit_str)
                    else:
                        ec3.metric("Rain Forecast", rain_label)


# ══════════════════════════════════════════════════════════
# TAB 2 — Leaderboard
# ══════════════════════════════════════════════════════════
with tab_leader:
    st.subheader(f"🏆 Last {days_window} Days — {selected_station}")

    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days_window)
    recent = df_eval[df_eval["Date"] >= cutoff] if not df_eval.empty else pd.DataFrame()

    if recent.empty:
        st.info(
            f"Not enough evaluated data in the last {days_window} days for **{selected_station}**.  \n"
            "Actuals are backfilled each morning — check back after the first few days."
        )
    else:
        lb = (
            recent.groupby("Source")
            .agg(
                Days=("Temp_Error", "count"),
                Max_Temp_MAE=("Temp_Error", "mean"),
                Min_Temp_MAE=("Temp_Min_Error", "mean"),
                Rain_MAE=("Rain_Error", "mean"),
                Rain_Hit_Pct=("Rain_Hit", lambda x: round(x.mean() * 100, 1)),
            )
            .reset_index()
            .round({"Max_Temp_MAE": 2, "Min_Temp_MAE": 2, "Rain_MAE": 2})
            .sort_values("Max_Temp_MAE")
            .reset_index(drop=True)
        )

        medals = ["🥇", "🥈", "🥉"] + [""] * max(0, len(lb) - 3)
        lb.insert(0, "Rank", medals[: len(lb)])

        st.dataframe(
            lb.rename(columns={
                "Max_Temp_MAE": "Max Temp MAE (°C)",
                "Min_Temp_MAE": "Min Temp MAE (°C)",
                "Rain_MAE": "Rain MAE (mm)",
                "Rain_Hit_Pct": "Rain Range Hit %",
            }),
            hide_index=True,
            use_container_width=True,
        )
        st.caption("MAE = Mean Absolute Error (lower = more accurate).  Rain Hit % = actual fell within forecast range.")

        # Bar chart
        if len(lb) > 1:
            fig_bar = go.Figure()
            for metric, label, color in [
                ("Max_Temp_MAE", "Max Temp MAE (°C)", "#1f77b4"),
                ("Rain_MAE", "Rain MAE (mm)", "#17becf"),
            ]:
                fig_bar.add_trace(go.Bar(
                    name=label,
                    x=lb["Source"],
                    y=lb[metric],
                    text=lb[metric],
                    textposition="outside",
                    marker_color=color,
                ))
            fig_bar.update_layout(
                title=f"Forecast Error Comparison — {selected_station} ({days_window}d)",
                barmode="group",
                yaxis_title="MAE",
                height=380,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # All-station summary
        st.divider()
        st.subheader("All Stations — Overall Leaderboard")

        df_all_eval = df[df["Source"].isin(selected_sources)].dropna(
            subset=["Actual_Max_Temp"]
        ).copy()
        if not df_all_eval.empty:
            df_all_eval["Temp_Error"] = abs(
                df_all_eval["Forecast_Max_Temp"] - df_all_eval["Actual_Max_Temp"]
            )
            df_all_recent = df_all_eval[df_all_eval["Date"] >= cutoff]
            if not df_all_recent.empty:
                lb_all = (
                    df_all_recent.groupby("Source")
                    .agg(Days=("Temp_Error", "count"), MAE=("Temp_Error", "mean"))
                    .reset_index()
                    .round({"MAE": 2})
                    .sort_values("MAE")
                    .reset_index(drop=True)
                )
                medals_all = ["🥇", "🥈", "🥉"] + [""] * max(0, len(lb_all) - 3)
                lb_all.insert(0, "Rank", medals_all[: len(lb_all)])
                lb_all.rename(columns={"MAE": "Max Temp MAE (°C)"}, inplace=True)
                st.dataframe(lb_all, hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════════════════
# TAB 3 — Trend Analysis
# ══════════════════════════════════════════════════════════
with tab_trend:
    st.subheader(f"📈 Error Trends — {selected_station}")

    if df_eval.empty:
        st.info("No evaluated data yet. Come back once actuals have been backfilled for at least one day.")
    else:
        df_trend = df_eval.sort_values("Date").copy()

        # Max temp absolute error over time
        fig_temp = px.line(
            df_trend,
            x="Date", y="Temp_Error",
            color="Source",
            color_discrete_map=SOURCE_COLORS,
            title=f"Max Temperature Absolute Error — {selected_station}",
            labels={"Temp_Error": "Abs Error (°C)", "Date": ""},
            markers=True,
        )
        fig_temp.update_layout(height=340, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig_temp, use_container_width=True)

        # Rain absolute error over time
        fig_rain = px.line(
            df_trend,
            x="Date", y="Rain_Error",
            color="Source",
            color_discrete_map=SOURCE_COLORS,
            title=f"Rainfall Absolute Error — {selected_station}",
            labels={"Rain_Error": "Abs Error (mm)", "Date": ""},
            markers=True,
        )
        fig_rain.update_layout(height=340, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig_rain, use_container_width=True)

        # Scatter: predicted vs actual max temp
        temp_min_v = df_eval["Actual_Max_Temp"].min()
        temp_max_v = df_eval["Actual_Max_Temp"].max()

        fig_scatter = px.scatter(
            df_eval,
            x="Actual_Max_Temp",
            y="Forecast_Max_Temp",
            color="Source",
            color_discrete_map=SOURCE_COLORS,
            title=f"Forecast vs Actual Max Temp — {selected_station}",
            labels={
                "Actual_Max_Temp": "Actual Max (°C)",
                "Forecast_Max_Temp": "Forecast Max (°C)",
            },
            hover_data=["Date", "Station"],
        )
        # Perfect-prediction diagonal line
        fig_scatter.add_trace(go.Scatter(
            x=[temp_min_v, temp_max_v],
            y=[temp_min_v, temp_max_v],
            mode="lines",
            name="Perfect forecast",
            line=dict(dash="dash", color="gray", width=1),
        ))
        fig_scatter.update_layout(
            height=420,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)


# ══════════════════════════════════════════════════════════
# TAB 4 — Raw Data
# ══════════════════════════════════════════════════════════
with tab_raw:
    st.subheader("📋 Raw Data")

    all_stations = sorted(df["Station"].dropna().unique().tolist())
    all_sources = sorted(df["Source"].dropna().unique().tolist())

    rc1, rc2 = st.columns(2)
    with rc1:
        raw_stations = st.multiselect("Station", all_stations, default=all_stations, key="raw_st")
    with rc2:
        raw_sources = st.multiselect("Source", all_sources, default=all_sources, key="raw_src")

    raw_df = df[
        df["Station"].isin(raw_stations) & df["Source"].isin(raw_sources)
    ].sort_values("Date", ascending=False)

    st.dataframe(
        raw_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
            "Forecast_Min_Temp": st.column_config.NumberColumn("F. Min (°C)", format="%.1f"),
            "Forecast_Max_Temp": st.column_config.NumberColumn("F. Max (°C)", format="%.1f"),
            "Forecast_Rain_Prob": st.column_config.NumberColumn("Rain % chance", format="%.0f"),
            "Forecast_Rain_Min_mm": st.column_config.NumberColumn("F. Rain Min (mm)", format="%.1f"),
            "Forecast_Rain_Max_mm": st.column_config.NumberColumn("F. Rain Max (mm)", format="%.1f"),
            "Actual_Min_Temp": st.column_config.NumberColumn("A. Min (°C)", format="%.1f"),
            "Actual_Max_Temp": st.column_config.NumberColumn("A. Max (°C)", format="%.1f"),
            "Actual_Rain_mm": st.column_config.NumberColumn("A. Rain (mm)", format="%.1f"),
        },
    )

    st.caption(f"{len(raw_df):,} rows shown")

    csv_bytes = raw_df.to_csv(index=False).encode()
    st.download_button(
        label="⬇️ Download CSV",
        data=csv_bytes,
        file_name=f"adelaide_weather_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
