"""
run_pipeline.py
---------------
MASTER PIPELINE ENTRY POINT
----------------------------
Runs the complete geo data pipeline end-to-end:
  Extract → Transform → Load → DQ Checks → Refresh Views

In production this is triggered by Apache Airflow (Control-M equivalent).
Locally it runs as a single Python script.

Usage:
    python run_pipeline.py          # Full run
    python run_pipeline.py --dq-only   # Re-run DQ checks only
"""

import sys
import os
import time
import argparse
from datetime import datetime
from loguru import logger

# ── Logger ─────────────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stdout,
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
           colorize=True)

os.makedirs("logs", exist_ok=True)
logger.add("logs/pipeline_{time:YYYYMMDD}.log", rotation="1 day", retention="30 days")


def init_schema(engine):
    """
    Creates all tables and indexes from the SQL DDL file.
    Each statement runs in its own transaction so a failing INDEX
    (e.g. table not yet visible) never blocks the TABLE creation.
    Safe to re-run — all statements use IF NOT EXISTS.
    """
    from sqlalchemy import text
    schema_path = os.path.join(os.path.dirname(__file__), "sql", "schema", "create_schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    # Strip comments and blank lines, split on semicolons
    statements = [
        s.strip() for s in schema_sql.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]

    # Pass 1: CREATE TABLE statements first
    for stmt in statements:
        if stmt.upper().startswith("CREATE TABLE"):
            try:
                with engine.begin() as conn:
                    conn.execute(text(stmt))
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.warning(f"Table creation warning: {e}")

    # Pass 2: CREATE INDEX statements after all tables exist
    for stmt in statements:
        if stmt.upper().startswith("CREATE INDEX"):
            try:
                with engine.begin() as conn:
                    conn.execute(text(stmt))
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.warning(f"Index creation warning: {e}")

    logger.success("Schema initialised")


def refresh_views(engine):
    """Drops and recreates all business question views."""
    from sqlalchemy import text
    views_path = os.path.join(os.path.dirname(__file__), "sql", "views", "business_views.sql")
    with open(views_path, "r") as f:
        views_sql = f.read()

    statements = [
        s.strip() for s in views_sql.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]
    with engine.begin() as conn:
        for stmt in statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                logger.warning(f"View warning: {e}")
    logger.success("Business views refreshed")


def run_full_pipeline():
    pipeline_start = time.time()
    logger.info("=" * 65)
    logger.info("  FNB GEO INTELLIGENCE PIPELINE")
    logger.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 65)

    # ── Step 0: DB connection ──────────────────────────────────────────────────
    logger.info("STEP 0 │ Connecting to database...")
    from pipeline.db_connection import get_engine, test_connection
    if not test_connection():
        logger.error("Database connection failed. Aborting.")
        sys.exit(1)
    engine = get_engine()

    # ── Step 0b: Schema init ───────────────────────────────────────────────────
    logger.info("STEP 0b │ Initialising schema...")
    init_schema(engine)

    # ── Step 1: Extract ────────────────────────────────────────────────────────
    logger.info("STEP 1 │ Extracting data from sources...")
    t = time.time()

    from pipeline.extract.extract_airports import extract_airports, extract_countries
    from pipeline.extract.extract_cities   import extract_cities, extract_country_info

    raw_airports    = extract_airports()
    raw_oa_countries = extract_countries()
    raw_cities      = extract_cities()
    raw_countryinfo = extract_country_info()

    logger.success(
        f"Extract done in {time.time()-t:.1f}s | "
        f"Airports: {len(raw_airports):,} | "
        f"Cities: {len(raw_cities):,} | "
        f"Countries: {len(raw_countryinfo):,}"
    )

    # ── Step 2: Transform ──────────────────────────────────────────────────────
    logger.info("STEP 2 │ Transforming data...")
    t = time.time()

    from pipeline.transform.transform_airports import transform_airports, transform_countries
    from pipeline.transform.transform_cities   import transform_cities, transform_country_info

    clean_airports,  quar_airports  = transform_airports(raw_airports)
    clean_oa_countries               = transform_countries(raw_oa_countries)
    clean_cities,    quar_cities    = transform_cities(raw_cities, raw_countryinfo)
    clean_country_info               = transform_country_info(raw_countryinfo)

    logger.success(
        f"Transform done in {time.time()-t:.1f}s | "
        f"Clean airports: {len(clean_airports):,} | "
        f"Quarantined: {len(quar_airports):,} | "
        f"Clean cities: {len(clean_cities):,}"
    )

    # ── Step 3: Load ───────────────────────────────────────────────────────────
    logger.info("STEP 3 │ Loading into star schema...")
    t = time.time()

    from pipeline.load.load_to_sqlite import (
        load_dim_continents, load_dim_facility_types,
        load_dim_country, load_fact_airport, load_fact_city,
        load_quarantine
    )

    load_dim_continents(engine)
    load_dim_facility_types(engine)
    load_dim_country(engine, clean_country_info)
    load_fact_airport(engine, clean_airports, clean_country_info)
    load_fact_city(engine, clean_cities)
    load_quarantine(engine, quar_airports, "quarantine_airport")
    load_quarantine(engine, quar_cities,   "quarantine_city")

    logger.success(f"Load done in {time.time()-t:.1f}s")

    # ── Step 4: DQ checks ──────────────────────────────────────────────────────
    logger.info("STEP 4 │ Running data quality checks...")
    from pipeline.quality.dq_checks import run_all_checks
    dq_results = run_all_checks(engine)

    # ── Step 5: Refresh views ──────────────────────────────────────────────────
    logger.info("STEP 5 │ Refreshing business views...")
    refresh_views(engine)

    # ── Done ───────────────────────────────────────────────────────────────────
    total      = time.time() - pipeline_start
    failures   = [r for r in dq_results if r["status"] == "FAIL"]

    logger.info("=" * 65)
    logger.success(f"  PIPELINE COMPLETE in {total:.1f}s")
    logger.info(f"  Airports loaded   : {len(clean_airports):,}")
    logger.info(f"  Cities loaded     : {len(clean_cities):,}")
    logger.info(f"  Countries loaded  : {len(clean_country_info):,}")
    logger.info(f"  DQ passed         : {len(dq_results)-len(failures)}/{len(dq_results)}")
    logger.info(f"  Database file     : geo_data.db")
    if failures:
        logger.error(f"  DQ FAILURES: {[r['check'] for r in failures]}")
    logger.info("=" * 65)

    return len(failures) == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FNB Geo Intelligence Pipeline")
    parser.add_argument("--dq-only", action="store_true",
                        help="Run DQ checks without re-downloading data")
    args = parser.parse_args()

    if args.dq_only:
        from pipeline.db_connection import get_engine
        from pipeline.quality.dq_checks import run_all_checks
        run_all_checks(get_engine())
    else:
        success = run_full_pipeline()
        sys.exit(0 if success else 1)
