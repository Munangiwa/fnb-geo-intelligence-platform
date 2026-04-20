-- =============================================================================
-- create_schema.sql  (SQLite compatible)
-- FNB Geo Intelligence Platform — Star Schema DDL
-- =============================================================================
-- DESIGN  : Star schema with conformed dimensions and quarantine tables.
--           Written for SQLite (demo). Production equivalent is Teradata/PostgreSQL.
--           The schema design is identical — only data types differ.
-- =============================================================================

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_continent (
    continent_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    continent_code  TEXT NOT NULL UNIQUE,
    continent_name  TEXT NOT NULL,
    load_date       TEXT NOT NULL DEFAULT (date('now'))
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code    TEXT NOT NULL UNIQUE,
    iso3            TEXT,
    country_name    TEXT NOT NULL,
    capital         TEXT,
    continent_code  TEXT,
    continent_name  TEXT,
    population      INTEGER DEFAULT 0,
    area_sq_km      REAL,
    load_date       TEXT NOT NULL DEFAULT (date('now'))
);

CREATE TABLE IF NOT EXISTS dim_facility_type (
    facility_type_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    facility_category   TEXT NOT NULL UNIQUE,
    description         TEXT
);

CREATE TABLE IF NOT EXISTS dim_settlement_type (
    settlement_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_code        TEXT NOT NULL UNIQUE,
    settlement_type     TEXT NOT NULL,
    description         TEXT
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact_airport (
    airport_sk          INTEGER PRIMARY KEY AUTOINCREMENT,
    airport_id          INTEGER,
    icao_code           TEXT,
    iata_code           TEXT,
    airport_name        TEXT NOT NULL,
    facility_category   TEXT,
    facility_type_raw   TEXT,
    latitude            REAL,
    longitude           REAL,
    elevation_ft        REAL,
    elevation_m         REAL,
    elevation_missing   INTEGER DEFAULT 0,
    country_code        TEXT,
    country_name        TEXT,
    continent_code      TEXT,
    continent_name      TEXT,
    region_code         TEXT,
    city                TEXT,
    load_date           TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_url          TEXT
);

CREATE INDEX IF NOT EXISTS idx_airport_country   ON fact_airport(country_code);
CREATE INDEX IF NOT EXISTS idx_airport_continent ON fact_airport(continent_code);
CREATE INDEX IF NOT EXISTS idx_airport_category  ON fact_airport(facility_category);
CREATE INDEX IF NOT EXISTS idx_airport_elevation ON fact_airport(elevation_m);

CREATE TABLE IF NOT EXISTS fact_city (
    city_sk             INTEGER PRIMARY KEY AUTOINCREMENT,
    geonameid           INTEGER NOT NULL,
    city_name           TEXT NOT NULL,
    city_name_ascii     TEXT,
    latitude            REAL,
    longitude           REAL,
    elevation_m         REAL,
    elevation_source    TEXT,
    elevation_missing   INTEGER DEFAULT 0,
    feature_code        TEXT,
    settlement_type     TEXT,
    country_code        TEXT,
    country_name        TEXT,
    continent_code      TEXT,
    continent_name      TEXT,
    admin1_code         TEXT,
    admin2_code         TEXT,
    population          INTEGER DEFAULT 0,
    timezone            TEXT,
    load_date           TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_url          TEXT
);

CREATE INDEX IF NOT EXISTS idx_city_country    ON fact_city(country_code);
CREATE INDEX IF NOT EXISTS idx_city_continent  ON fact_city(continent_code);
CREATE INDEX IF NOT EXISTS idx_city_population ON fact_city(population);
CREATE INDEX IF NOT EXISTS idx_city_elevation  ON fact_city(elevation_m);

-- =============================================================================
-- QUARANTINE TABLES  (bad data is NEVER silently dropped)
-- =============================================================================

CREATE TABLE IF NOT EXISTS quarantine_airport (
    quarantine_sk     INTEGER PRIMARY KEY AUTOINCREMENT,
    airport_id        INTEGER,
    airport_name      TEXT,
    facility_type     TEXT,
    country_code      TEXT,
    quarantine_reason TEXT,
    quarantine_date   TEXT DEFAULT (date('now'))
);

CREATE TABLE IF NOT EXISTS quarantine_city (
    quarantine_sk     INTEGER PRIMARY KEY AUTOINCREMENT,
    geonameid         INTEGER,
    city_name         TEXT,
    country_code      TEXT,
    quarantine_reason TEXT,
    quarantine_date   TEXT DEFAULT (date('now'))
);

-- =============================================================================
-- DATA QUALITY LOG
-- =============================================================================

CREATE TABLE IF NOT EXISTS dq_results (
    dq_sk            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date         TEXT NOT NULL,
    run_timestamp    TEXT NOT NULL DEFAULT (datetime('now')),
    table_name       TEXT NOT NULL,
    check_name       TEXT NOT NULL,
    check_type       TEXT,
    records_checked  INTEGER,
    records_passed   INTEGER,
    records_failed   INTEGER,
    pass_rate_pct    REAL,
    status           TEXT,
    notes            TEXT
);

-- =============================================================================
-- PIPELINE AUDIT LOG
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_sk              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
    task_name           TEXT,
    status              TEXT,
    records_read        INTEGER,
    records_loaded      INTEGER,
    records_quarantined INTEGER,
    duration_seconds    REAL,
    error_message       TEXT
);
