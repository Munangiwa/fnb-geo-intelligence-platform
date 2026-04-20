"""
transform_cities.py
-------------------
LAYER   : Staging / Conformed
PURPOSE : Clean and standardise raw GeoNames city data.

GeoNames elevation strategy:
  - 'elevation' column = SRTM precise (~90m resolution) — use first
  - 'dem' column       = GTOPO30 fallback (~900m resolution)
  - Both null          = flagged as elevation_missing, record kept
"""

import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger

RUN_DATE = datetime.now().date()

CONTINENT_MAP = {
    "AF": "Africa",
    "AS": "Asia",
    "EU": "Europe",
    "NA": "North America",
    "OC": "Oceania",
    "SA": "South America",
    "AN": "Antarctica"
}

CITY_FEATURE_CODES = {
    "PPL"  : "Populated Place",
    "PPLA" : "Provincial Capital",
    "PPLA2": "District Capital",
    "PPLA3": "Sub-District Capital",
    "PPLA4": "Sub-Sub-District Capital",
    "PPLC" : "National Capital",
    "PPLX" : "Settlement Section",
    "PPLG" : "Seat of Government",
    "PPLS" : "Populated Places",
    "PPLF" : "Farm Village",
    "PPLL" : "Populated Locality",
    "PPLR" : "Religious Populated Place",
}


def transform_cities(raw_df: pd.DataFrame, country_info_df: pd.DataFrame) -> tuple:
    """
    Transforms raw GeoNames cities into conformed staging format.
    Returns (clean_df, quarantine_df).
    """
    logger.info(f"Transforming cities: {len(raw_df):,} raw records")
    df = raw_df.copy()

    # Filter to populated places only (feature_class = P)
    df = df[df["feature_class"] == "P"].copy()
    logger.info(f"Populated places only: {len(df):,}")

    # Map feature codes to readable settlement types
    df["settlement_type"] = df["feature_code"].map(CITY_FEATURE_CODES).fillna("Settlement")

    # Join continent from GeoNames countryInfo
    country_ref = country_info_df[["iso", "continent", "country"]].rename(columns={
        "iso"    : "country_code",
        "country": "country_name"
    })
    df = df.merge(country_ref, on="country_code", how="left")
    df["continent_name"] = df["continent"].map(CONTINENT_MAP).fillna("Unknown")

    # Elevation: prefer SRTM, fall back to DEM
    df["elevation_m"]      = pd.to_numeric(df["elevation"], errors="coerce")
    df["dem"]              = pd.to_numeric(df["dem"],       errors="coerce")
    df["elevation_source"] = np.where(df["elevation_m"].notna(), "srtm", "dem")
    df["elevation_m"]      = df["elevation_m"].fillna(df["dem"])
    df["elevation_missing"] = df["elevation_m"].isna().astype(int)

    missing = df["elevation_missing"].sum()
    if missing > 0:
        logger.warning(f"{missing:,} cities missing elevation — flagged")

    # Quarantine records with no country code
    quarantine_mask = df["country_code"].isna() | (df["country_code"].astype(str).str.strip() == "")
    quarantine_df   = df[quarantine_mask].copy()
    quarantine_df["quarantine_reason"] = "Missing country_code"
    quarantine_df["quarantine_date"]   = str(RUN_DATE)
    df = df[~quarantine_mask].copy()

    # Standardise
    df["name"]         = df["name"].str.strip()
    df["asciiname"]    = df["asciiname"].str.strip()
    df["country_code"] = df["country_code"].str.strip().str.upper()
    df["population"]   = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)

    # Audit columns
    df["load_date"]     = str(RUN_DATE)
    df["source_system"] = "geonames.org"
    df["source_url"]    = "https://download.geonames.org/export/dump/cities15000.zip"

    logger.success(f"City transform complete: {len(df):,} clean | {len(quarantine_df):,} quarantined")
    return df, quarantine_df


def transform_country_info(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms GeoNames countryInfo into the DIM_COUNTRY dimension."""
    df = raw_df[[
        "iso", "iso3", "country", "capital",
        "area_sq_km", "population", "continent"
    ]].rename(columns={
        "iso"    : "country_code",
        "country": "country_name"
    }).copy()

    df["country_code"]   = df["country_code"].str.strip().str.upper()
    df["continent_code"] = df["continent"].str.strip()
    df["continent_name"] = df["continent_code"].map(CONTINENT_MAP).fillna("Unknown")
    df["population"]     = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)
    df["area_sq_km"]     = pd.to_numeric(df["area_sq_km"], errors="coerce")
    df["load_date"]      = str(datetime.now().date())

    logger.success(f"Country info transformed: {len(df):,} countries")
    return df
