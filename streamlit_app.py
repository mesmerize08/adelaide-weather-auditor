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
from datetime import datetime

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

# WMO IDs for live BOM observation fetching
STATION_WMO = {
    "West Terrace": "94648",
    "Airport": "94672",
    "Mt Lofty": "94693",
}

SOURCE_COLORS = {
    "BOM": "#1f77b4",
    "Open-Meteo": "#2ca02c",
    "Weatherzone": "#d62728",
}

# Threshold for flagging source disagreement (°C std dev across sources)
DISAGREEMENT_THRESHOLD = 3.0

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
# Live BOM observations (30-min cached — "right now" data)
# ─────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def fetch_live_obs(wmo_id: str) -> dict | None:
    """Fetch the latest BOM observation for a station via WMO ID."""
    url = f"http://www.bom.gov.au/fwo/IDS60901/IDS60901.{wmo_id}.json"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()["observations"]["data"]
        if not data:
            return None
        latest = data[0]
        return {
            "temp": latest.get("air_temp"),
            "apparent_temp": latest.get("apparent_t"),
            "humidity": latest.get("rel_hum"),
            "wind_kmh": latest.get("wind_spd_kmh"),
            "wind_dir": latest.get("wind_dir"),
            "rain_since_9am": latest.get("rain_trace"),
            "local_time": latest.get("local_date_time_full"),
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────
# Streak computation helper
# ─────────────────────────────────────────────────────────
def compute_streaks(df_eval: pd.DataFrame) -> dict:
    """
    Find which source has been most accurate (lowest max-temp MAE) for the
    most consecutive recent days.  Returns a dict with current winner info.
    """
    if df_eval.empty:
        return {}

    daily = (
        df_eval.groupby(["Date", "Source"])["Temp_Error"]
        .mean()
        .reset_index()
    )
    # Winner for each day = source with min error
    idx = daily.groupby("Date")["Temp_Error"].idxmin()
    winners = daily.loc[idx][["Date", "Source"]].sort_values("Date", ascending=False)

    if winners.empty:
        return {}

    # Current streak — walk backwards from most recent day
    dates = winners["Date"].tolist()
    sources = winners["Source"].tolist()
    current_source = sources[0]
    streak = 1
    for i in range(1, len(dates)):
        # Only count consecutive calendar days
        gap = (dates[i - 1] - dates[i]).days
        if gap > 1:
            break
        if sources[i] == current_source:
            streak += 1
        else:
            break

    return {
        "source": current_source,
        "days": streak,
        "since": dates[streak - 1].strftime("%d %b %Y") if streak <= len(dates) else "—",
    }


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
# Live "Right Now" panel
# ─────────────────────────────────────────────────────────
wmo_id = STATION_WMO.get(selected_station)
live_obs = fetch_live_obs(wmo_id) if wmo_id else None

with st.expander("⚡ Live Conditions Right Now — " + selected_station, expanded=True):
    if live_obs and live_obs.get("temp") is not None:
        obs_time = live_obs.get("local_time", "")
        if obs_time:
            try:
                obs_dt = datetime.strptime(str(obs_time), "%Y%m%d%H%M%S")
                obs_label = obs_dt.strftime("%I:%M %p, %d %b")
            except Exception:
                obs_label = str(obs_time)
        else:
            obs_label = "latest reading"

        l1, l2, l3, l4, l5 = st.columns(5)
        l1.metric("🌡️ Temp", f"{live_obs['temp']} °C")
        l2.metric(
            "🤔 Feels Like",
            f"{live_obs['apparent_temp']} °C" if live_obs.get("apparent_temp") is not None else "—",
        )
        l3.metric(
            "💧 Humidity",
            f"{live_obs['humidity']}%" if live_obs.get("humidity") is not None else "—",
        )
        wind_str = (
            f"{live_obs['wind_kmh']} km/h {live_obs['wind_dir']}"
            if live_obs.get("wind_kmh") is not None else "—"
        )
        l4.metric("🌬️ Wind", wind_str)
        rain_raw = live_obs.get("rain_since_9am")
        rain_str = (
            "Trace" if str(rain_raw).lower() == "trace"
            else f"{rain_raw} mm" if rain_raw not in (None, "-", "") else "0 mm"
        )
        l5.metric("🌧️ Rain since 9am", rain_str)
        st.caption(f"BOM observation as of {obs_label} · Auto-refreshes every 30 min")
    else:
        st.info(
            f"Live BOM observations unavailable for **{selected_station}** right now.  "
            "BOM's observation feed may be temporarily unreachable."
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
    # Binary: did it actually rain?
    df_eval["Rain_Day"] = df_eval["Actual_Rain_mm"] > 0
    # Did source predict rain (prob > 30%)?
    df_eval["Rain_Predicted"] = df_eval["Forecast_Rain_Prob"].fillna(0) > 30
    # Brier Score term per row: (forecast_prob/100 − outcome)²
    # Only rows with a non-null probability contribute; rest stay NaN.
    has_prob = df_eval["Forecast_Rain_Prob"].notna()
    df_eval["Brier_Term"] = float("nan")
    df_eval.loc[has_prob, "Brier_Term"] = (
        df_eval.loc[has_prob, "Forecast_Rain_Prob"] / 100
        - df_eval.loc[has_prob, "Rain_Day"].astype(float)
    ) ** 2

# ─────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────
tab_cal, tab_leader, tab_trend, tab_monthly, tab_raw = st.tabs([
    "📅 Calendar View",
    "🏆 Leaderboard",
    "📈 Trend Analysis",
    "🗓️ Monthly Summary",
    "📋 Raw Data",
])


# ══════════════════════════════════════════════════════════
# TAB 1 — Calendar / Day Inspector
# ══════════════════════════════════════════════════════════
with tab_cal:

    # ── Error heatmap ─────────────────────────────────────
    st.subheader("🔥 Forecast Error Heatmap")
    if df_eval.empty:
        st.info("Heatmap will appear once actuals have been backfilled for at least one day.")
    else:
        heat_df = (
            df_eval.groupby(["Date", "Source"])["Temp_Error"]
            .mean()
            .reset_index()
        )
        # Pivot: rows = Source, columns = Date
        heat_pivot = heat_df.pivot(index="Source", columns="Date", values="Temp_Error")
        date_labels = [d.strftime("%d %b") for d in heat_pivot.columns]

        fig_heat = go.Figure(go.Heatmap(
            z=heat_pivot.values,
            x=date_labels,
            y=heat_pivot.index.tolist(),
            colorscale=[
                [0.0, "#2ca02c"],   # green = accurate
                [0.5, "#ffdd57"],   # yellow = moderate error
                [1.0, "#d62728"],   # red = large error
            ],
            colorbar=dict(title="Abs Error (°C)"),
            hovertemplate="Date: %{x}<br>Source: %{y}<br>Error: %{z:.1f}°C<extra></extra>",
        ))
        fig_heat.update_layout(
            title=f"Max Temp Absolute Error — {selected_station}",
            height=220,
            margin=dict(t=50, b=30, l=10, r=10),
            xaxis=dict(side="bottom"),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    # ── Source disagreement indicator ──────────────────────
    st.subheader("⚠️ Source Disagreement Days")
    disagree_df = (
        df[df["Station"] == selected_station]
        .groupby("Date")["Forecast_Max_Temp"]
        .std()
        .reset_index()
        .rename(columns={"Forecast_Max_Temp": "Spread_Std"})
        .dropna()
    )
    flagged = disagree_df[disagree_df["Spread_Std"] >= DISAGREEMENT_THRESHOLD].copy()
    flagged["Date_str"] = flagged["Date"].dt.strftime("%d %b %Y")

    if flagged.empty:
        st.success(
            f"No days where sources disagreed by more than {DISAGREEMENT_THRESHOLD}°C (std).  "
            "All sources are broadly aligned."
        )
    else:
        st.warning(
            f"**{len(flagged)} day(s)** where source spread exceeded **{DISAGREEMENT_THRESHOLD}°C** (std of max-temp forecasts):"
        )
        for _, row in flagged.sort_values("Date", ascending=False).iterrows():
            # Show per-source forecasts for that day
            day_fc = df[
                (df["Date"] == row["Date"]) & (df["Station"] == selected_station)
            ][["Source", "Forecast_Max_Temp"]].dropna()
            fc_parts = "  ·  ".join(
                f"{r['Source']}: {r['Forecast_Max_Temp']:.1f}°C"
                for _, r in day_fc.iterrows()
            )
            st.markdown(f"**{row['Date_str']}** — std {row['Spread_Std']:.1f}°C · {fc_parts}")

    st.divider()

    # ── Day inspector ─────────────────────────────────────
    st.subheader("🔍 Day Inspector")
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

            # Disagreement badge for this day
            day_spread = day_data["Forecast_Max_Temp"].std()
            if pd.notna(day_spread) and day_spread >= DISAGREEMENT_THRESHOLD:
                st.warning(f"⚠️ Sources disagreed significantly on this day (σ = {day_spread:.1f}°C)")

            st.divider()

            for _, row in day_data.iterrows():
                source = row["Source"]
                f_max = row.get("Forecast_Max_Temp")
                f_min = row.get("Forecast_Min_Temp")
                r_min = row.get("Forecast_Rain_Min_mm", 0) or 0
                r_max = row.get("Forecast_Rain_Max_mm", 0) or 0
                r_prob = row.get("Forecast_Rain_Prob")

                with st.expander(f"**{source}**", expanded=True):
                    ec1, ec2, ec3 = st.columns(3)

                    if has_actuals and pd.notna(f_max):
                        err = f_max - actual_max
                        ec1.metric(
                            "Forecast Max", f"{f_max:.1f} °C",
                            f"{err:+.1f} °C",
                            delta_color="inverse",
                        )
                    else:
                        ec1.metric("Forecast Max", f"{f_max:.1f} °C" if pd.notna(f_max) else "—")

                    if has_actuals and pd.notna(f_min) and pd.notna(actual_min):
                        err_min = f_min - actual_min
                        ec2.metric(
                            "Forecast Min", f"{f_min:.1f} °C",
                            f"{err_min:+.1f} °C",
                            delta_color="inverse",
                        )
                    else:
                        ec2.metric("Forecast Min", f"{f_min:.1f} °C" if pd.notna(f_min) else "—")

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

    # ── Streak tracker ─────────────────────────────────────
    streak = compute_streaks(df_eval)
    if streak:
        st.info(
            f"🔥 **{streak['source']}** has been the most accurate source for "
            f"**{streak['days']} consecutive day(s)** (since {streak['since']}) — "
            f"Max Temp MAE · {selected_station}"
        )

    st.subheader(f"🏆 Last {days_window} Days — {selected_station}")

    # Use Adelaide time so "last N days" is correct regardless of server timezone
    cutoff = pd.Timestamp.now(tz="Australia/Adelaide").tz_localize(None) - pd.Timedelta(days=days_window)
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

    # ── Enhanced rain accuracy ─────────────────────────────
    st.divider()
    st.subheader("🌧️ Rain Accuracy Breakdown")

    if not df_eval.empty and "Rain_Day" in df_eval.columns:
        rain_stats_rows = []
        rain_source = recent if not recent.empty else df_eval
        for src, grp in rain_source.groupby("Source"):
            total = len(grp)
            rain_days = grp["Rain_Day"].sum()
            dry_days = total - rain_days

            # Hit rate on actual rain days
            rain_day_hits = grp[grp["Rain_Day"]]["Rain_Hit"].sum() if rain_days > 0 else 0
            rain_hit_rate = round(100 * rain_day_hits / rain_days, 1) if rain_days > 0 else None

            # False alarm rate: predicted rain but it was dry
            false_alarms = grp[~grp["Rain_Day"] & grp["Rain_Predicted"]].shape[0]
            fa_rate = round(100 * false_alarms / dry_days, 1) if dry_days > 0 else None

            # Miss rate: did not predict rain but it rained
            misses = grp[grp["Rain_Day"] & ~grp["Rain_Predicted"]].shape[0]
            miss_rate = round(100 * misses / rain_days, 1) if rain_days > 0 else None

            # Brier Score: mean (prob/100 - outcome)² across rows with a probability
            brier_grp = grp.dropna(subset=["Brier_Term"])
            brier_score = round(brier_grp["Brier_Term"].mean(), 4) if not brier_grp.empty else None

            rain_stats_rows.append({
                "Source": src,
                "Evaluated Days": total,
                "Rain Days": int(rain_days),
                "Dry Days": int(dry_days),
                "Hit Rate (rain days) %": rain_hit_rate,
                "False Alarm Rate %": fa_rate,
                "Miss Rate %": miss_rate,
                "Brier Score ↓": brier_score,
            })

        if rain_stats_rows:
            rain_stats_df = pd.DataFrame(rain_stats_rows)
            st.dataframe(rain_stats_df, hide_index=True, use_container_width=True)
            st.caption(
                "**Hit Rate** = actual rain fell within forecast range on rain days.  "
                "**False Alarm** = predicted rain (>30% prob) but it stayed dry.  "
                "**Miss Rate** = rain actually fell but wasn't predicted.  "
                "**Brier Score** = mean (forecast prob − outcome)² — lower is better. "
                "Rewards confident correct predictions (0% on dry days, 100% on rain days) "
                "and penalises hedging (e.g. always predicting 5–10% 'just in case'). "
                "Perfect score = 0.00, always-wrong = 1.00."
            )
        else:
            st.info("Rain accuracy breakdown available once sufficient data is collected.")
    else:
        st.info("Rain accuracy breakdown will appear once actuals are available.")

    # ── All-station summary ────────────────────────────────
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

        temp_min_v = df_eval["Actual_Max_Temp"].min()
        temp_max_v = df_eval["Actual_Max_Temp"].max()
        # If only one data point, extend the diagonal slightly so it renders
        if temp_min_v == temp_max_v:
            temp_min_v -= 1
            temp_max_v += 1

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
# TAB 4 — Monthly Summary Grid
# ══════════════════════════════════════════════════════════
with tab_monthly:
    st.subheader(f"🗓️ Monthly Summary — {selected_station}")

    # Need at least one evaluated row
    df_eval_all = df[
        (df["Station"] == selected_station)
        & (df["Source"].isin(selected_sources))
    ].dropna(subset=["Actual_Max_Temp"]).copy()

    if df_eval_all.empty:
        st.info("Monthly summary will populate once actuals have been backfilled for at least one full day.")
    else:
        df_eval_all["Temp_Error"] = abs(
            df_eval_all["Forecast_Max_Temp"] - df_eval_all["Actual_Max_Temp"]
        )
        df_eval_all["Month"] = df_eval_all["Date"].dt.to_period("M")

        # Per source per month — avg MAE + days
        monthly = (
            df_eval_all.groupby(["Month", "Source"])
            .agg(Avg_MAE=("Temp_Error", "mean"), Days=("Temp_Error", "count"))
            .reset_index()
        )

        # Best source per month (lowest avg MAE)
        idx_best = monthly.groupby("Month")["Avg_MAE"].idxmin()
        best_per_month = monthly.loc[idx_best][["Month", "Source"]].rename(
            columns={"Source": "Best_Source"}
        )

        # Count wins per source per month (just 1 win for the best)
        # and pivot MAE table
        mae_pivot = monthly.pivot(index="Month", columns="Source", values="Avg_MAE").round(2)
        mae_pivot = mae_pivot.reset_index()
        mae_pivot["Month"] = mae_pivot["Month"].astype(str)
        mae_pivot = mae_pivot.merge(
            best_per_month.assign(Month=best_per_month["Month"].astype(str)),
            on="Month",
            how="left",
        )
        mae_pivot = mae_pivot.sort_values("Month", ascending=False)

        st.markdown("**Average Max Temp MAE (°C) by Month and Source**")
        st.dataframe(mae_pivot, hide_index=True, use_container_width=True)
        st.caption("Best_Source = source with lowest average Max Temp MAE that month. Lower MAE = more accurate.")

        # Bar chart: MAE per month stacked by source
        if len(monthly["Source"].unique()) > 0 and len(monthly["Month"].unique()) > 0:
            monthly_plot = monthly.copy()
            monthly_plot["Month"] = monthly_plot["Month"].astype(str)
            fig_monthly = px.bar(
                monthly_plot,
                x="Month",
                y="Avg_MAE",
                color="Source",
                barmode="group",
                color_discrete_map=SOURCE_COLORS,
                title=f"Monthly Avg Max Temp MAE — {selected_station}",
                labels={"Avg_MAE": "Avg MAE (°C)", "Month": ""},
                text="Avg_MAE",
            )
            fig_monthly.update_traces(texttemplate="%{text:.2f}", textposition="outside")
            fig_monthly.update_layout(
                height=380,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_monthly, use_container_width=True)

        # Win tally
        st.divider()
        st.markdown("**Monthly Wins (most accurate source each month)**")
        win_counts = best_per_month["Best_Source"].value_counts().reset_index()
        win_counts.columns = ["Source", "Monthly Wins"]
        st.dataframe(win_counts, hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════════════════
# TAB 5 — Raw Data
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
