-- =============================================================================
-- business_views.sql  (SQLite compatible)
-- FNB Geo Intelligence Platform — Business Question Views
-- =============================================================================
-- Each view answers one business question.
-- Users query views only — never the underlying fact/dimension tables directly.
-- New questions = new views. Zero pipeline changes required.
-- SQLite uses DROP VIEW IF EXISTS before each CREATE VIEW.
-- =============================================================================

-- =============================================================================
-- Q1: Airports, airfields and heliports per country and continent
-- =============================================================================
DROP VIEW IF EXISTS vw_q1_facility_count_by_country;
CREATE VIEW vw_q1_facility_count_by_country AS
SELECT
    fa.continent_name,
    fa.country_name,
    fa.country_code,
    fa.facility_category                    AS facility_type,
    COUNT(*)                                AS facility_count
FROM fact_airport fa
WHERE fa.facility_category IS NOT NULL
GROUP BY
    fa.continent_name,
    fa.country_name,
    fa.country_code,
    fa.facility_category
ORDER BY fa.continent_name, fa.country_name, fa.facility_category;

DROP VIEW IF EXISTS vw_q1_facility_count_by_continent;
CREATE VIEW vw_q1_facility_count_by_continent AS
SELECT
    continent_name,
    facility_category                       AS facility_type,
    COUNT(*)                                AS facility_count
FROM fact_airport
WHERE facility_category IS NOT NULL
GROUP BY continent_name, facility_category
ORDER BY continent_name, facility_category;

-- =============================================================================
-- Q2: Average elevation of airports, airfields and heliports per country
-- =============================================================================
DROP VIEW IF EXISTS vw_q2_avg_elevation_by_country;
CREATE VIEW vw_q2_avg_elevation_by_country AS
SELECT
    fa.continent_name,
    fa.country_name,
    fa.country_code,
    fa.facility_category                    AS facility_type,
    COUNT(*)                                AS total_facilities,
    COUNT(fa.elevation_m)                   AS facilities_with_elevation,
    ROUND(AVG(fa.elevation_m), 1)           AS avg_elevation_m,
    ROUND(MIN(fa.elevation_m), 1)           AS min_elevation_m,
    ROUND(MAX(fa.elevation_m), 1)           AS max_elevation_m
FROM fact_airport fa
WHERE fa.elevation_missing = 0
GROUP BY
    fa.continent_name,
    fa.country_name,
    fa.country_code,
    fa.facility_category
ORDER BY fa.continent_name, fa.country_name;

-- =============================================================================
-- Q3: Estimated population of each country
-- =============================================================================
DROP VIEW IF EXISTS vw_q3_country_population;
CREATE VIEW vw_q3_country_population AS
SELECT
    dc.continent_name,
    dc.country_code,
    dc.country_name,
    dc.capital,
    dc.population                           AS country_population,
    dc.area_sq_km,
    CASE
        WHEN dc.area_sq_km > 0
        THEN ROUND(CAST(dc.population AS REAL) / dc.area_sq_km, 2)
        ELSE NULL
    END                                     AS population_density_per_sqkm,
    dc.load_date                            AS data_as_of
FROM dim_country dc
ORDER BY dc.continent_name, dc.population DESC;

-- =============================================================================
-- Q4: Cities/towns/settlements per country
-- =============================================================================
DROP VIEW IF EXISTS vw_q4_settlement_count_by_country;
CREATE VIEW vw_q4_settlement_count_by_country AS
SELECT
    fc.continent_name,
    fc.country_name,
    fc.country_code,
    fc.settlement_type,
    COUNT(*)                                AS settlement_count,
    SUM(fc.population)                      AS total_population_in_settlements
FROM fact_city fc
GROUP BY
    fc.continent_name,
    fc.country_name,
    fc.country_code,
    fc.settlement_type
ORDER BY fc.continent_name, fc.country_name, fc.settlement_type;

DROP VIEW IF EXISTS vw_q4_total_settlements_by_country;
CREATE VIEW vw_q4_total_settlements_by_country AS
SELECT
    fc.continent_name,
    fc.country_name,
    fc.country_code,
    COUNT(*)                                AS total_settlements
FROM fact_city fc
GROUP BY
    fc.continent_name,
    fc.country_name,
    fc.country_code
ORDER BY total_settlements DESC;

-- =============================================================================
-- Q5: Min, max and average elevation of cities per country
-- =============================================================================
DROP VIEW IF EXISTS vw_q5_city_elevation_stats_by_country;
CREATE VIEW vw_q5_city_elevation_stats_by_country AS
SELECT
    fc.continent_name,
    fc.country_name,
    fc.country_code,
    COUNT(*)                                AS total_cities,
    COUNT(fc.elevation_m)                   AS cities_with_elevation,
    ROUND(MIN(fc.elevation_m), 1)           AS min_elevation_m,
    ROUND(MAX(fc.elevation_m), 1)           AS max_elevation_m,
    ROUND(AVG(fc.elevation_m), 1)           AS avg_elevation_m
FROM fact_city fc
WHERE fc.elevation_missing = 0
GROUP BY
    fc.continent_name,
    fc.country_name,
    fc.country_code
ORDER BY avg_elevation_m DESC;

-- =============================================================================
-- Q6: Highest and lowest elevated cities with population > 100,000
-- =============================================================================
DROP VIEW IF EXISTS vw_q6_highest_cities_pop100k;
CREATE VIEW vw_q6_highest_cities_pop100k AS
SELECT
    'Highest'                               AS category,
    fc.city_name,
    fc.country_name,
    fc.continent_name,
    fc.population,
    fc.elevation_m,
    fc.latitude,
    fc.longitude
FROM fact_city fc
WHERE fc.population > 100000
  AND fc.elevation_missing = 0
ORDER BY fc.elevation_m DESC
LIMIT 20;

DROP VIEW IF EXISTS vw_q6_lowest_cities_pop100k;
CREATE VIEW vw_q6_lowest_cities_pop100k AS
SELECT
    'Lowest'                                AS category,
    fc.city_name,
    fc.country_name,
    fc.continent_name,
    fc.population,
    fc.elevation_m,
    fc.latitude,
    fc.longitude
FROM fact_city fc
WHERE fc.population > 100000
  AND fc.elevation_missing = 0
ORDER BY fc.elevation_m ASC
LIMIT 20;

-- =============================================================================
-- Q7: Highest and lowest airports, airfields and heliports on the planet
-- =============================================================================
DROP VIEW IF EXISTS vw_q7_highest_airports;
CREATE VIEW vw_q7_highest_airports AS
SELECT
    'Highest'                               AS category,
    fa.airport_name,
    fa.facility_category,
    fa.country_name,
    fa.continent_name,
    fa.city,
    fa.elevation_m,
    fa.elevation_ft,
    fa.icao_code,
    fa.iata_code,
    fa.latitude,
    fa.longitude
FROM fact_airport fa
WHERE fa.elevation_missing = 0
ORDER BY fa.elevation_m DESC
LIMIT 20;

DROP VIEW IF EXISTS vw_q7_lowest_airports;
CREATE VIEW vw_q7_lowest_airports AS
SELECT
    'Lowest'                                AS category,
    fa.airport_name,
    fa.facility_category,
    fa.country_name,
    fa.continent_name,
    fa.city,
    fa.elevation_m,
    fa.elevation_ft,
    fa.icao_code,
    fa.iata_code,
    fa.latitude,
    fa.longitude
FROM fact_airport fa
WHERE fa.elevation_missing = 0
ORDER BY fa.elevation_m ASC
LIMIT 20;

-- =============================================================================
-- Global summary KPIs — powers the dashboard header cards
-- =============================================================================
DROP VIEW IF EXISTS vw_global_summary;
CREATE VIEW vw_global_summary AS
SELECT
    (SELECT COUNT(DISTINCT country_code) FROM dim_country)                      AS total_countries,
    (SELECT COUNT(*) FROM fact_airport WHERE facility_category = 'Airport')     AS total_airports,
    (SELECT COUNT(*) FROM fact_airport WHERE facility_category = 'Airfield')    AS total_airfields,
    (SELECT COUNT(*) FROM fact_airport WHERE facility_category = 'Heliport')    AS total_heliports,
    (SELECT COUNT(*) FROM fact_city)                                             AS total_settlements,
    (SELECT SUM(population) FROM dim_country)                                   AS world_population,
    (SELECT MAX(elevation_m) FROM fact_airport WHERE elevation_missing = 0)     AS highest_airport_m,
    (SELECT MIN(elevation_m) FROM fact_airport WHERE elevation_missing = 0)     AS lowest_airport_m,
    (SELECT MAX(elevation_m) FROM fact_city    WHERE elevation_missing = 0)     AS highest_city_m,
    (SELECT MIN(elevation_m) FROM fact_city    WHERE elevation_missing = 0)     AS lowest_city_m,
    date('now')                                                                  AS data_as_of;
