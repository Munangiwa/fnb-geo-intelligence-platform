# FNB Geo Intelligence Platform

A production-grade data engineering solution for geographic analysis.  
Built for the FNB Data Engineer II technical assessment.

---

## Quickstart (3 commands)

```bash
pip install -r requirements.txt
cp .env.example .env
python run_pipeline.py
streamlit run dashboard/app.py
```

Dashboard opens at **http://localhost:8501**

---

## Architecture

```
[DATA SOURCES]                  [LANDING]            [STAR SCHEMA]            [PRESENTATION]
OurAirports  (nightly CSV)  →   Raw files        →   fact_airport         →   Streamlit Dashboard
GeoNames     (daily ZIP)    →   with datestamp   →   fact_city            →   SQL Business Views
                                                 →   dim_country
                                                 →   dim_continent
                                                 →   quarantine tables
                                                 →   dq_results
                                                 →   pipeline_run_log
```

### Tool mapping to FNB production stack

| This demo       | FNB production equivalent |
|-----------------|--------------------------|
| Apache Airflow  | Control-M                |
| SQLite          | Teradata / PostgreSQL    |
| Python + Pandas | Ab Initio graphs         |
| Streamlit       | Power BI / Tableau       |
| Git             | Source control           |

> SQLite requires zero installation — the entire database is a single file (`geo_data.db`).  
> Swapping to PostgreSQL or Teradata = one line change in `pipeline/db_connection.py`.

---

## Project Structure

```
fnb_geo_solution/
├── run_pipeline.py              ← single entry point — runs everything
├── requirements.txt
├── .env.example                 ← copy to .env
├── .gitignore
├── README.md
│
├── pipeline/
│   ├── db_connection.py         ← centralised DB connection (SQLite)
│   ├── extract/
│   │   ├── extract_airports.py  ← downloads from OurAirports (nightly)
│   │   └── extract_cities.py    ← downloads from GeoNames (daily)
│   ├── transform/
│   │   ├── transform_airports.py
│   │   └── transform_cities.py
│   ├── load/
│   │   └── load_to_sqlite.py
│   └── quality/
│       └── dq_checks.py         ← 12 automated DQ checks
│
├── sql/
│   ├── schema/
│   │   └── create_schema.sql    ← star schema DDL (SQLite)
│   └── views/
│       └── business_views.sql   ← all 7 business questions as SQL views
│
├── dags/
│   └── geo_pipeline_dag.py      ← Airflow DAG = Control-M job chain
│
└── dashboard/
    └── app.py                   ← Streamlit interactive dashboard
```

---

## Data Sources

| Source | URL | Refresh |
|--------|-----|---------|
| OurAirports airports.csv | https://ourairports.com/data/ | Nightly |
| GeoNames cities15000.zip | https://www.geonames.org/export/ | Daily |
| GeoNames countryInfo.txt | https://www.geonames.org/export/ | Daily |

---

## Data Model (Star Schema)

```
dim_continent ──┐
                ├── dim_country ──── fact_airport  (~74,000 rows)
                │               └── fact_city      (~25,000 rows)
                └── dim_facility_type

Audit tables:
    dq_results          ← 12 DQ check results per run
    pipeline_run_log    ← audit trail for every execution
    quarantine_airport  ← rejected records with reason
    quarantine_city     ← rejected records with reason
```

---

## Business Questions — View Reference

| # | Question | View |
|---|----------|------|
| Q1 | Facility counts by country | `vw_q1_facility_count_by_country` |
| Q1 | Facility counts by continent | `vw_q1_facility_count_by_continent` |
| Q2 | Avg elevation by country | `vw_q2_avg_elevation_by_country` |
| Q3 | Country populations | `vw_q3_country_population` |
| Q4 | Settlement counts | `vw_q4_total_settlements_by_country` |
| Q5 | City elevation stats | `vw_q5_city_elevation_stats_by_country` |
| Q6 | Extreme cities (pop > 100k) | `vw_q6_highest/lowest_cities_pop100k` |
| Q7 | Extreme airports | `vw_q7_highest/lowest_airports` |
| — | Global KPI summary | `vw_global_summary` |

---

## Adding New Questions

New business questions require only:
1. Add a SQL view to `sql/views/business_views.sql`
2. Add a page to `dashboard/app.py`
3. **Zero pipeline changes required** — this is the star schema benefit

---

## Pipeline Commands

```bash
# Full pipeline run (download → transform → load → DQ → views)
python run_pipeline.py

# DQ checks only (no re-download)
python run_pipeline.py --dq-only

# Launch dashboard
streamlit run dashboard/app.py
```

---

## Git Setup

```bash
git init
git add .
git commit -m "feat: FNB geo intelligence platform (SQLite + Streamlit)"
git remote add origin https://github.com/YOUR_USERNAME/fnb-geo-solution.git
git push -u origin main
```
