"""
load_to_sqlite.py
-----------------
LAYER   : EDW Load
PURPOSE : Load transformed DataFrames into the SQLite star schema.
          Uses full refresh strategy (DELETE + INSERT) wrapped in transactions.
          Production equivalent: Teradata MERGE / UPSERT for delta loads.

STRATEGY: Full refresh is used because:
  - OurAirports updates nightly (~12MB — fast to reload)
  - GeoNames cities15000 updates daily (~4MB)
  - Full refresh guarantees consistency and is easy to audit
  - Delta load can be layered on once baseline is stable
"""

import pandas as pd
from datetime import datetime
from loguru import logger
from sqlalchemy import text
from pipeline.db_connection import get_engine

RUN_DATE = str(datetime.now().date())


def _log_run(engine, task: str, status: str, loaded=0, quarantined=0, duration=0.0, error=None):
    """Writes an entry to the pipeline audit log."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_run_log
                (task_name, status, records_loaded, records_quarantined,
                 duration_seconds, error_message)
            VALUES (:task, :status, :loaded, :quar, :dur, :err)
        """), {
            "task"  : task,   "status": status,
            "loaded": loaded, "quar"  : quarantined,
            "dur"   : duration, "err" : str(error) if error else None
        })


def load_dim_continents(engine):
    """Seeds the continent dimension — 7 continents."""
    continents = [
        ("AF", "Africa"), ("AS", "Asia"), ("EU", "Europe"),
        ("NA", "North America"), ("OC", "Oceania"),
        ("SA", "South America"), ("AN", "Antarctica"), ("UN", "Unknown"),
    ]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_continent"))
        conn.execute(
            text("INSERT INTO dim_continent (continent_code, continent_name) VALUES (:c, :n)"),
            [{"c": code, "n": name} for code, name in continents]
        )
    logger.success(f"dim_continent loaded: {len(continents)} rows")


def load_dim_facility_types(engine):
    """Seeds the facility type dimension."""
    types = [
        ("Airport",  "Commercial or private airport with paved runway"),
        ("Airfield", "Small or unpaved airstrip, seaplane base, or balloonport"),
        ("Heliport", "Helicopter landing facility"),
    ]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_facility_type"))
        conn.execute(
            text("INSERT INTO dim_facility_type (facility_category, description) VALUES (:c, :d)"),
            [{"c": cat, "d": desc} for cat, desc in types]
        )
    logger.success(f"dim_facility_type loaded: {len(types)} rows")


def load_dim_country(engine, country_df: pd.DataFrame):
    """Loads the country dimension. Full refresh."""
    start = datetime.now()
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM dim_country"))

        df = country_df.copy()
        if "continent" in df.columns and "continent_code" not in df.columns:
            df = df.rename(columns={"continent": "continent_code"})

        df["load_date"] = RUN_DATE

        cols = ["country_code", "iso3", "country_name", "capital",
                "continent_code", "continent_name", "population", "area_sq_km", "load_date"]
        cols = [c for c in cols if c in df.columns]

        df[cols].to_sql("dim_country", engine, if_exists="append", index=False,
                        method="multi", chunksize=500)

        duration = (datetime.now() - start).total_seconds()
        logger.success(f"dim_country loaded: {len(df):,} rows in {duration:.1f}s")
        _log_run(engine, "load_dim_country", "SUCCESS", loaded=len(df), duration=duration)

    except Exception as e:
        logger.error(f"dim_country load failed: {e}")
        _log_run(engine, "load_dim_country", "FAILED", error=e)
        raise


def load_fact_airport(engine, airports_df: pd.DataFrame, country_ref_df: pd.DataFrame):
    """
    Loads the airport fact table.
    Enriches airports with continent info by joining to the country reference.
    """
    start = datetime.now()
    try:
        # Enrich with continent
        df = airports_df.merge(
            country_ref_df[["country_code", "country_name", "continent_code", "continent_name"]],
            on="country_code", how="left"
        )

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_airport"))

        df = df.rename(columns={
            "name"              : "airport_name",
            "type"              : "facility_type_raw",
        })

        cols = [
            "airport_id", "icao_code", "iata_code",
            "airport_name", "facility_category", "facility_type_raw",
            "latitude", "longitude",
            "elevation_ft", "elevation_m", "elevation_missing",
            "country_code", "country_name", "continent_code", "continent_name",
            "region_code", "city",
            "load_date", "source_system", "source_url"
        ]
        cols = [c for c in cols if c in df.columns]

        df[cols].to_sql("fact_airport", engine, if_exists="append", index=False,
                        method="multi", chunksize=1000)

        duration = (datetime.now() - start).total_seconds()
        logger.success(f"fact_airport loaded: {len(df):,} rows in {duration:.1f}s")
        _log_run(engine, "load_fact_airport", "SUCCESS", loaded=len(df), duration=duration)

    except Exception as e:
        logger.error(f"fact_airport load failed: {e}")
        _log_run(engine, "load_fact_airport", "FAILED", error=e)
        raise


def load_fact_city(engine, cities_df: pd.DataFrame):
    """Loads the city fact table."""
    start = datetime.now()
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_city"))

        df = cities_df.rename(columns={
            "name"     : "city_name",
            "asciiname": "city_name_ascii",
        })

        if "continent" in df.columns and "continent_code" not in df.columns:
            df = df.rename(columns={"continent": "continent_code"})

        cols = [
            "geonameid", "city_name", "city_name_ascii",
            "latitude", "longitude",
            "elevation_m", "elevation_source", "elevation_missing",
            "feature_code", "settlement_type",
            "country_code", "country_name", "continent_code", "continent_name",
            "admin1_code", "admin2_code",
            "population", "timezone",
            "load_date", "source_system", "source_url"
        ]
        cols = [c for c in cols if c in df.columns]

        df[cols].to_sql("fact_city", engine, if_exists="append", index=False,
                        method="multi", chunksize=1000)

        duration = (datetime.now() - start).total_seconds()
        logger.success(f"fact_city loaded: {len(df):,} rows in {duration:.1f}s")
        _log_run(engine, "load_fact_city", "SUCCESS", loaded=len(df), duration=duration)

    except Exception as e:
        logger.error(f"fact_city load failed: {e}")
        _log_run(engine, "load_fact_city", "FAILED", error=e)
        raise


def load_quarantine(engine, df: pd.DataFrame, table: str):
    """Appends quarantined records to the audit quarantine table."""
    if df is None or len(df) == 0:
        return
    try:
        df["quarantine_date"] = RUN_DATE
        cols = {
            "quarantine_airport": ["airport_id", "airport_name", "facility_type_raw",
                                   "country_code", "quarantine_reason", "quarantine_date"],
            "quarantine_city":    ["geonameid", "city_name", "country_code",
                                   "quarantine_reason", "quarantine_date"],
        }
        save_cols = [c for c in cols.get(table, []) if c in df.columns]
        if save_cols:
            df[save_cols].to_sql(table, engine, if_exists="append", index=False)
            logger.warning(f"{table}: {len(df):,} quarantined records saved")
    except Exception as e:
        logger.warning(f"Quarantine load warning ({table}): {e}")
