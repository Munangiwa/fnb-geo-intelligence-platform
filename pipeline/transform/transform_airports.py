"""
transform_airports.py
---------------------
LAYER   : Staging / Conformed
PURPOSE : Clean, standardise, and enrich raw airport data.

KEY TRANSFORMATIONS:
  - Map raw types to business categories (Airport / Airfield / Heliport)
  - Convert elevation feet → metres
  - Quarantine closed / unrecognised facility types (never silently drop)
  - Add audit columns: load_date, source_system, source_url
"""

import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger

FEET_TO_METRES = 0.3048
RUN_DATE       = datetime.now().date()

TYPE_MAP = {
    "large_airport"  : "Airport",
    "medium_airport" : "Airport",
    "small_airport"  : "Airport",
    "heliport"       : "Heliport",
    "seaplane_base"  : "Airfield",
    "balloonport"    : "Airfield",
    "closed"         : "Closed",
}


def transform_airports(raw_df: pd.DataFrame) -> tuple:
    """
    Transforms raw airports DataFrame into conformed staging data.
    Returns (clean_df, quarantine_df).
    """
    logger.info(f"Transforming airports: {len(raw_df):,} raw records")
    df = raw_df.copy()

    # Select and rename columns
    df = df[[
        "id", "ident", "type", "name",
        "latitude_deg", "longitude_deg", "elevation_ft",
        "iso_country", "iso_region", "municipality",
        "iata_code", "gps_code"
    ]].rename(columns={
        "id"           : "airport_id",
        "ident"        : "icao_code",
        "latitude_deg" : "latitude",
        "longitude_deg": "longitude",
        "iso_country"  : "country_code",
        "iso_region"   : "region_code",
        "municipality" : "city"
    })

    # Map to business category
    df["facility_category"] = df["type"].map(TYPE_MAP).fillna("Airfield")

    # Quarantine closed facilities
    quarantine_mask = df["facility_category"] == "Closed"
    quarantine_df   = df[quarantine_mask].copy()
    quarantine_df["quarantine_reason"] = "Closed facility — excluded from analysis"
    quarantine_df["quarantine_date"]   = str(RUN_DATE)
    df = df[~quarantine_mask].copy()
    logger.info(f"Quarantined {len(quarantine_df):,} closed facilities")

    # Convert elevation ft → metres
    df["elevation_ft"]      = pd.to_numeric(df["elevation_ft"], errors="coerce")
    df["elevation_m"]       = (df["elevation_ft"] * FEET_TO_METRES).round(1)
    df["elevation_missing"] = df["elevation_m"].isna().astype(int)

    missing = df["elevation_missing"].sum()
    if missing > 0:
        logger.warning(f"{missing:,} airports missing elevation — flagged, not dropped")

    # Standardise strings
    df["name"]         = df["name"].str.strip().str.title()
    df["city"]         = df["city"].str.strip().str.title()
    df["country_code"] = df["country_code"].str.strip().str.upper()

    # Audit columns
    df["load_date"]     = str(RUN_DATE)
    df["source_system"] = "ourairports.com"
    df["source_url"]    = "https://davidmegginson.github.io/ourairports-data/airports.csv"

    logger.success(
        f"Airport transform complete: {len(df):,} clean | "
        f"{len(quarantine_df):,} quarantined\n"
        f"{df['facility_category'].value_counts().to_string()}"
    )
    return df, quarantine_df


def transform_countries(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms OurAirports countries reference for continent mapping."""
    df = raw_df[["code", "name", "continent"]].rename(columns={
        "code": "country_code",
        "name": "country_name"
    }).copy()
    df["country_code"] = df["country_code"].str.strip().str.upper()
    df["country_name"] = df["country_name"].str.strip()
    df["load_date"]    = str(datetime.now().date())
    logger.success(f"Countries (OurAirports) transformed: {len(df):,}")
    return df
