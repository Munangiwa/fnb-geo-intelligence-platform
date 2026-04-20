"""
extract_cities.py
-----------------
LAYER   : Landing / Raw
PURPOSE : Download cities and country population data from GeoNames.
          GeoNames provides daily exports — data is always current.

SOURCE  : https://www.geonames.org/export/
FILES   : cities15000.zip  — all populated places with population > 15,000
          countryInfo.txt  — country-level population, continent, capital
"""

import os
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

CITIES_URL      = os.getenv("CITIES_URL",      "https://download.geonames.org/export/dump/cities15000.zip")
COUNTRYINFO_URL = os.getenv("COUNTRYINFO_URL", "https://download.geonames.org/export/dump/countryInfo.txt")

LANDING_DIR = os.path.join(os.path.dirname(__file__), "../../data/landing")
os.makedirs(LANDING_DIR, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y%m%d")

GEONAMES_COLUMNS = [
    "geonameid", "name", "asciiname", "alternatenames",
    "latitude", "longitude", "feature_class", "feature_code",
    "country_code", "cc2", "admin1_code", "admin2_code",
    "admin3_code", "admin4_code", "population", "elevation",
    "dem", "timezone", "modification_date"
]

COUNTRYINFO_COLUMNS = [
    "iso", "iso3", "iso_numeric", "fips", "country", "capital",
    "area_sq_km", "population", "continent", "tld", "currency_code",
    "currency_name", "phone", "postal_code_format", "postal_code_regex",
    "languages", "geonameid", "neighbours", "equivalent_fips_code"
]


def extract_cities() -> pd.DataFrame:
    """Downloads cities15000.zip, extracts tab-delimited data, returns DataFrame."""
    logger.info(f"Downloading cities: {CITIES_URL}")
    try:
        response = requests.get(CITIES_URL, timeout=300, stream=True)
        response.raise_for_status()

        zip_path = os.path.join(LANDING_DIR, f"cities15000_raw_{RUN_DATE}.zip")
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        with zipfile.ZipFile(zip_path, "r") as z:
            with z.open("cities15000.txt") as txt_file:
                df = pd.read_csv(
                    txt_file, sep="\t", header=None,
                    names=GEONAMES_COLUMNS, low_memory=False, encoding="utf-8"
                )

        logger.success(f"Cities extracted: {len(df):,} records")
        return df
    except Exception as e:
        logger.error(f"Cities extraction failed: {e}")
        raise


def extract_country_info() -> pd.DataFrame:
    """Downloads countryInfo.txt — country populations, continents, capitals."""
    logger.info(f"Downloading country info: {COUNTRYINFO_URL}")
    try:
        response = requests.get(COUNTRYINFO_URL, timeout=60)
        response.raise_for_status()

        raw_path = os.path.join(LANDING_DIR, f"countryinfo_raw_{RUN_DATE}.txt")
        with open(raw_path, "wb") as f:
            f.write(response.content)

        lines      = response.content.decode("utf-8").splitlines()
        data_lines = [l for l in lines if not l.startswith("#") and l.strip()]

        df = pd.read_csv(
            io.StringIO("\n".join(data_lines)),
            sep="\t", header=None,
            names=COUNTRYINFO_COLUMNS, low_memory=False
        )
        logger.success(f"Country info extracted: {len(df):,} countries")
        return df
    except Exception as e:
        logger.error(f"Country info extraction failed: {e}")
        raise


if __name__ == "__main__":
    cities  = extract_cities()
    country = extract_country_info()
    print(cities[["name", "country_code", "population", "elevation"]].head())
    print(country[["iso", "country", "continent", "population"]].head())
