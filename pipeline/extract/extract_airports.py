"""
extract_airports.py
-------------------
LAYER   : Landing / Raw
PURPOSE : Download airports.csv and countries.csv from OurAirports.
          Updated nightly by OurAirports — every run pulls the freshest data.

SOURCE  : https://ourairports.com/data/
FILES   : airports.csv  — all airports, airfields, heliports worldwide (~74k records)
          countries.csv — country codes and continent mapping
"""

import os
import requests
import pandas as pd
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

AIRPORTS_URL  = os.getenv("AIRPORTS_URL",  "https://davidmegginson.github.io/ourairports-data/airports.csv")
COUNTRIES_URL = os.getenv("COUNTRIES_URL", "https://davidmegginson.github.io/ourairports-data/countries.csv")

LANDING_DIR = os.path.join(os.path.dirname(__file__), "../../data/landing")
os.makedirs(LANDING_DIR, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y%m%d")


def download_file(url: str, filename: str) -> str:
    """Downloads a file and saves to the landing zone with datestamp for audit trail."""
    local_path = os.path.join(LANDING_DIR, filename)
    logger.info(f"Downloading: {url}")
    try:
        response = requests.get(url, timeout=120, stream=True)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        logger.success(f"Downloaded → {local_path} ({size_mb:.2f} MB)")
        return local_path
    except requests.RequestException as e:
        logger.error(f"Download failed for {url}: {e}")
        raise


def extract_airports() -> pd.DataFrame:
    """Downloads and returns raw airports data as a DataFrame."""
    path = download_file(AIRPORTS_URL, f"airports_raw_{RUN_DATE}.csv")
    df   = pd.read_csv(path, low_memory=False)
    logger.info(f"Airports extracted: {len(df):,} records")
    logger.info(f"Types: {df['type'].value_counts().to_dict()}")
    return df


def extract_countries() -> pd.DataFrame:
    """Downloads and returns raw OurAirports countries reference."""
    path = download_file(COUNTRIES_URL, f"countries_raw_{RUN_DATE}.csv")
    df   = pd.read_csv(path, low_memory=False)
    logger.info(f"Countries (OurAirports) extracted: {len(df):,} records")
    return df


if __name__ == "__main__":
    ap = extract_airports()
    co = extract_countries()
    print(ap[["ident", "name", "type", "iso_country", "elevation_ft"]].head())
    print(co.head())
