"""
app.py — FNB Geo Intelligence Dashboard
Run: streamlit run dashboard/app.py
"""

import os
import sys
import subprocess
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pipeline.db_connection import get_engine

st.set_page_config(
    page_title="FNB Geo Intelligence Platform",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1a237e 0%, #283593 100%);
        padding: 1rem; border-radius: 10px; color: white;
        text-align: center; margin: 0.25rem;
    }
    .metric-value { font-size: 2rem; font-weight: bold; color: #FFD700; }
    .metric-label { font-size: 0.85rem; opacity: 0.85; }
    h2 { color: #1a237e; border-bottom: 3px solid #FFD700; padding-bottom: 0.4rem; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db():
    return get_engine()


@st.cache_data(ttl=300)
def query(_engine, sql: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def trigger_refresh():
    """Runs the pipeline live — the 'latest data' demo moment."""
    # project_root = os.path.join(os.path.dirname(__file__), "..")
    # result = subprocess.run(
    #     [sys.executable, "run_pipeline.py"],
    #     capture_output=True, text=True, cwd=project_root
    # )
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        script_path = os.path.join(project_root, "run_pipeline.py")

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=300  # prevent hanging
        )
    return result.returncode == 0, result.stdout + result.stderr


# ═══════════════════════════════════════════════════════════════════════════════
def main():
    # ── Header ─────────────────────────────────────────────────────────────────
    col_title, col_btn = st.columns([5, 1])
    with col_title:
        st.title("🌍 FNB Geo Intelligence Platform")
        st.caption("Strategic geographic analysis — Board of Directors Safety Planning")
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh Data", type="primary",
                     help="Pull latest data from OurAirports & GeoNames right now"):
            with st.spinner("Downloading latest data and rebuilding pipeline..."):
                ok, log = trigger_refresh()
            if ok:
                st.success("✅ Data refreshed with latest available data!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Pipeline failed.")
                with st.expander("View pipeline log"):
                    st.code(log)

    # ── Check DB ───────────────────────────────────────────────────────────────
    try:
        engine = get_db()
    except Exception as e:
        st.error(f"❌ Database not ready: {e}")
        st.code("python run_pipeline.py", language="bash")
        return

    # ── Check if data exists ───────────────────────────────────────────────────
    try:
        check = query(engine, "SELECT COUNT(*) AS n FROM fact_airport")
        if check.iloc[0]["n"] == 0:
            st.warning("No data loaded yet. Run `python run_pipeline.py` first.")
            return
    except Exception:
        st.warning("Tables not found. Run `python run_pipeline.py` first.")
        return

    # ── Global KPI Cards ───────────────────────────────────────────────────────
    try:
        s = query(engine, "SELECT * FROM vw_global_summary").iloc[0]
        st.caption(f"Data as of: **{s.get('data_as_of', 'unknown')}**")

        cols = st.columns(6)
        kpis = [
            ("✈️ Airports",    f"{int(s['total_airports']):,}"),
            ("🛬 Airfields",   f"{int(s['total_airfields']):,}"),
            ("🚁 Heliports",   f"{int(s['total_heliports']):,}"),
            ("🏙️ Settlements", f"{int(s['total_settlements']):,}"),
            ("🌍 Countries",   f"{int(s['total_countries']):,}"),
            ("👥 World Pop",   f"{int(s['world_population'])/1e9:.2f}B"),
        ]
        for col, (label, value) in zip(cols, kpis):
            with col:
                st.markdown(f"""
                <div class='metric-card'>
                    <div class='metric-label'>{label}</div>
                    <div class='metric-value'>{value}</div>
                </div>""", unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"Could not load KPIs: {e}")

    st.markdown("---")

    # ── Sidebar ────────────────────────────────────────────────────────────────
    st.sidebar.title("📊 Navigation")
    page = st.sidebar.radio("Business Question:", [
        "Q1 — Facility Counts",
        "Q2 — Airport Elevations",
        "Q3 — Country Populations",
        "Q4 — Settlement Counts",
        "Q5 — City Elevations",
        "Q6 — Extreme Cities",
        "Q7 — Extreme Airports",
        "📋 Data Quality",
    ])

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Filter**")
    try:
        cont_df = query(engine,
            "SELECT DISTINCT continent_name FROM dim_continent WHERE continent_name != 'Unknown' ORDER BY continent_name")
        selected = st.sidebar.selectbox("Continent", ["All"] + cont_df["continent_name"].tolist())
    except Exception:
        selected = "All"

    cont_filter = f"WHERE continent_name = '{selected}'" if selected != "All" else ""
    cont_and    = f"AND continent_name = '{selected}'"   if selected != "All" else ""

    # ═══════════════════════════════════════════════════════════════════════════
    # Q1 — FACILITY COUNTS
    # ═══════════════════════════════════════════════════════════════════════════
    if page == "Q1 — Facility Counts":
        st.header("Q1 — Airports, Airfields & Heliports per Country / Continent")

        tab1, tab2 = st.tabs(["By Continent", "By Country"])

        with tab1:
            df = query(engine, "SELECT * FROM vw_q1_facility_count_by_continent ORDER BY continent_name")
            fig = px.bar(df, x="continent_name", y="facility_count", color="facility_type",
                         barmode="group", title="Facility Count by Continent",
                         color_discrete_map={"Airport": "#1a237e", "Airfield": "#FFD700", "Heliport": "#e53935"},
                         labels={"continent_name": "Continent", "facility_count": "Count", "facility_type": "Type"})
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

        with tab2:
            df = query(engine, f"""
                SELECT * FROM vw_q1_facility_count_by_country
                {cont_filter}
                ORDER BY facility_count DESC LIMIT 50
            """)
            fig = px.bar(df.head(30), x="country_name", y="facility_count",
                         color="facility_type", title="Top 30 Countries by Facility Count",
                         color_discrete_map={"Airport": "#1a237e", "Airfield": "#FFD700", "Heliport": "#e53935"})
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # Q2 — AIRPORT ELEVATIONS
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "Q2 — Airport Elevations":
        st.header("Q2 — Average Elevation of Facilities per Country")
        df = query(engine, f"""
            SELECT * FROM vw_q2_avg_elevation_by_country
            {cont_filter}
            ORDER BY avg_elevation_m DESC
        """)
        fig = px.scatter(df, x="country_name", y="avg_elevation_m", color="facility_type",
                         size="total_facilities",
                         hover_data=["min_elevation_m", "max_elevation_m"],
                         title="Average Facility Elevation by Country (size = facility count)",
                         color_discrete_map={"Airport": "#1a237e", "Airfield": "#FFD700", "Heliport": "#e53935"})
        fig.update_xaxes(tickangle=90)
        st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🏔️ Top 10 Highest Avg Elevation")
            st.dataframe(df.nlargest(10, "avg_elevation_m")
                         [["country_name", "facility_type", "avg_elevation_m", "total_facilities"]],
                         use_container_width=True, hide_index=True)
        with c2:
            st.markdown("##### 🌊 Top 10 Lowest Avg Elevation")
            st.dataframe(df.nsmallest(10, "avg_elevation_m")
                         [["country_name", "facility_type", "avg_elevation_m", "total_facilities"]],
                         use_container_width=True, hide_index=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # Q3 — COUNTRY POPULATIONS
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "Q3 — Country Populations":
        st.header("Q3 — Estimated Population per Country")
        df = query(engine, f"""
            SELECT * FROM vw_q3_country_population
            {cont_filter}
            ORDER BY country_population DESC
        """)
        fig = px.treemap(df.head(80),
                         path=["continent_name", "country_name"],
                         values="country_population",
                         title="World Population Distribution",
                         color="country_population",
                         color_continuous_scale="Blues")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            df[["continent_name", "country_name", "capital",
                "country_population", "area_sq_km", "population_density_per_sqkm"]],
            use_container_width=True, hide_index=True
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # Q4 — SETTLEMENT COUNTS
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "Q4 — Settlement Counts":
        st.header("Q4 — Cities / Towns / Settlements per Country")
        df = query(engine, f"""
            SELECT * FROM vw_q4_total_settlements_by_country
            {cont_filter}
            ORDER BY total_settlements DESC LIMIT 40
        """)
        fig = px.bar(df, x="country_name", y="total_settlements",
                     color="continent_name",
                     title="Top 40 Countries by Number of Settlements")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # Q5 — CITY ELEVATIONS
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "Q5 — City Elevations":
        st.header("Q5 — Min / Max / Avg Elevation of Cities per Country")
        df = query(engine, f"""
            SELECT * FROM vw_q5_city_elevation_stats_by_country
            {cont_filter}
            ORDER BY avg_elevation_m DESC
        """)
        fig = go.Figure()
        d30 = df.head(40)
        fig.add_trace(go.Bar(name="Max", x=d30["country_name"], y=d30["max_elevation_m"], marker_color="#e53935"))
        fig.add_trace(go.Bar(name="Avg", x=d30["country_name"], y=d30["avg_elevation_m"], marker_color="#1a237e"))
        fig.add_trace(go.Bar(name="Min", x=d30["country_name"], y=d30["min_elevation_m"], marker_color="#FFD700"))
        fig.update_layout(barmode="group", title="City Elevation Stats — Top 40 Countries by Average Elevation",
                          xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # Q6 — EXTREME CITIES
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "Q6 — Extreme Cities":
        st.header("Q6 — Highest & Lowest Cities with Population > 100,000")

        high = query(engine, "SELECT * FROM vw_q6_highest_cities_pop100k")
        low  = query(engine, "SELECT * FROM vw_q6_lowest_cities_pop100k")
        df   = pd.concat([high, low], ignore_index=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🏔️ Highest Elevated Cities")
            st.dataframe(high[["city_name", "country_name", "continent_name",
                                "elevation_m", "population"]],
                         use_container_width=True, hide_index=True)
        with c2:
            st.markdown("##### 🌊 Lowest Elevated Cities")
            st.dataframe(low[["city_name", "country_name", "continent_name",
                               "elevation_m", "population"]],
                         use_container_width=True, hide_index=True)

        fig = px.scatter_geo(df, lat="latitude", lon="longitude",
                             color="category", size="population",
                             hover_name="city_name",
                             hover_data={"elevation_m": True, "country_name": True},
                             title="Extreme Cities (population > 100,000)",
                             color_discrete_map={"Highest": "#e53935", "Lowest": "#1a237e"})
        st.plotly_chart(fig, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # Q7 — EXTREME AIRPORTS
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "Q7 — Extreme Airports":
        st.header("Q7 — Highest & Lowest Airports / Airfields / Heliports on Earth")

        high = query(engine, "SELECT * FROM vw_q7_highest_airports")
        low  = query(engine, "SELECT * FROM vw_q7_lowest_airports")
        df   = pd.concat([high, low], ignore_index=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🏔️ Highest Facilities")
            st.dataframe(high[["airport_name", "facility_category", "country_name",
                                "elevation_m", "icao_code"]],
                         use_container_width=True, hide_index=True)
        with c2:
            st.markdown("##### 🌊 Lowest Facilities")
            st.dataframe(low[["airport_name", "facility_category", "country_name",
                               "elevation_m", "icao_code"]],
                         use_container_width=True, hide_index=True)

        fig = px.scatter_geo(df, lat="latitude", lon="longitude",
                             color="category", symbol="facility_category",
                             hover_name="airport_name",
                             hover_data={"elevation_m": True, "country_name": True,
                                         "facility_category": True},
                             title="World's Most Extreme Aviation Facilities",
                             color_discrete_map={"Highest": "#e53935", "Lowest": "#1a237e"})
        st.plotly_chart(fig, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # DATA QUALITY
    # ═══════════════════════════════════════════════════════════════════════════
    elif page == "📋 Data Quality":
        st.header("📋 Data Quality Monitoring")
        try:
            dq = query(engine, """
                SELECT * FROM dq_results
                WHERE run_date = date('now')
                ORDER BY status DESC, table_name
            """)
            if len(dq) == 0:
                st.info("No DQ checks run today yet. Click 🔄 Refresh Data or run `python run_pipeline.py`.")
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("✅ Passed",   len(dq[dq["status"] == "PASS"]))
                c2.metric("⚠️ Warnings", len(dq[dq["status"] == "WARNING"]))
                c3.metric("❌ Failed",   len(dq[dq["status"] == "FAIL"]))

                def colour(val):
                    m = {"PASS": "background-color:#c8e6c9",
                         "WARNING": "background-color:#fff9c4",
                         "FAIL": "background-color:#ffcdd2"}
                    return m.get(val, "")

                st.dataframe(
                    dq[["table_name", "check_name", "check_type",
                        "records_checked", "records_failed", "pass_rate_pct", "status", "notes"]]
                    .style.map(colour, subset=["status"]),
                    use_container_width=True, hide_index=True
                )

            st.markdown("### Pipeline Run History")
            runs = query(engine, """
                SELECT run_timestamp, task_name, status, records_loaded, duration_seconds
                FROM pipeline_run_log ORDER BY run_timestamp DESC LIMIT 20
            """)
            st.dataframe(runs, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"DQ dashboard error: {e}")


if __name__ == "__main__":
    main()
