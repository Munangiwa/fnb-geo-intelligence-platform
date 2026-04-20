"""
dq_checks.py
------------
LAYER   : Data Quality
PURPOSE : Run automated DQ checks after every load.
          All results written to dq_results table — never silently swallowed.
          Bad data is visible, tracked, and reportable.

CHECK TYPES:
  COMPLETENESS — required fields populated?
  UNIQUENESS   — natural keys unique?
  VALIDITY     — values within expected ranges?
"""

from datetime import datetime
from loguru import logger
from sqlalchemy import text
from pipeline.db_connection import get_engine

RUN_DATE = str(datetime.now().date())


def run_check(engine, table: str, check_name: str, check_type: str,
              total_sql: str, failed_sql: str, notes: str = "") -> dict:
    """Runs one DQ check and writes result to dq_results."""
    with engine.begin() as conn:
        total  = conn.execute(text(total_sql)).scalar()  or 0
        failed = conn.execute(text(failed_sql)).scalar() or 0
        passed = total - failed
        rate   = round((passed / total * 100), 2) if total > 0 else 0.0
        status = "PASS" if failed == 0 else ("WARNING" if rate >= 95 else "FAIL")

        conn.execute(text("""
            INSERT INTO dq_results
                (run_date, table_name, check_name, check_type,
                 records_checked, records_passed, records_failed,
                 pass_rate_pct, status, notes)
            VALUES
                (:run_date, :table, :check, :type,
                 :checked, :passed, :failed, :rate, :status, :notes)
        """), {
            "run_date": RUN_DATE, "table"  : table,
            "check"   : check_name, "type": check_type,
            "checked" : total,    "passed" : passed,
            "failed"  : failed,   "rate"   : rate,
            "status"  : status,   "notes"  : notes
        })

    icon = "✅" if status == "PASS" else ("⚠️" if status == "WARNING" else "❌")
    logger.info(f"{icon} [{status}] {table} | {check_name}: {passed:,}/{total:,} ({rate}%)")
    return {"check": check_name, "status": status, "pass_rate": rate, "table": table}


def run_all_checks(engine=None) -> list:
    if engine is None:
        engine = get_engine()

    results = []
    logger.info("=" * 60)
    logger.info("Running Data Quality Checks")
    logger.info("=" * 60)

    # ── fact_airport ──────────────────────────────────────────────────────────

    results.append(run_check(engine,
        "fact_airport", "ROW_COUNT_MINIMUM", "VALIDITY",
        "SELECT COUNT(*) FROM fact_airport",
        "SELECT CASE WHEN COUNT(*) < 10000 THEN 1 ELSE 0 END FROM fact_airport",
        "Expect at least 10,000 records globally"
    ))

    results.append(run_check(engine,
        "fact_airport", "NULL_AIRPORT_NAME", "COMPLETENESS",
        "SELECT COUNT(*) FROM fact_airport",
        "SELECT COUNT(*) FROM fact_airport WHERE airport_name IS NULL OR airport_name = ''"
    ))

    results.append(run_check(engine,
        "fact_airport", "NULL_COUNTRY_CODE", "COMPLETENESS",
        "SELECT COUNT(*) FROM fact_airport",
        "SELECT COUNT(*) FROM fact_airport WHERE country_code IS NULL"
    ))

    results.append(run_check(engine,
        "fact_airport", "ELEVATION_COVERAGE", "COMPLETENESS",
        "SELECT COUNT(*) FROM fact_airport",
        "SELECT COUNT(*) FROM fact_airport WHERE elevation_missing = 1",
        "Flag if >20% missing elevation"
    ))

    results.append(run_check(engine,
        "fact_airport", "ELEVATION_RANGE_VALID", "VALIDITY",
        "SELECT COUNT(*) FROM fact_airport WHERE elevation_missing = 0",
        "SELECT COUNT(*) FROM fact_airport WHERE elevation_m < -500 OR elevation_m > 6000",
        "Valid range: -500m (Dead Sea area) to 6000m (Daocheng Yading)"
    ))

    results.append(run_check(engine,
        "fact_airport", "VALID_FACILITY_CATEGORY", "VALIDITY",
        "SELECT COUNT(*) FROM fact_airport",
        "SELECT COUNT(*) FROM fact_airport WHERE facility_category NOT IN ('Airport','Airfield','Heliport')"
    ))

    results.append(run_check(engine,
        "fact_airport", "DUPLICATE_AIRPORT_ID", "UNIQUENESS",
        "SELECT COUNT(*) FROM fact_airport",
        "SELECT COUNT(*) - COUNT(DISTINCT airport_id) FROM fact_airport"
    ))

    # ── fact_city ─────────────────────────────────────────────────────────────

    results.append(run_check(engine,
        "fact_city", "ROW_COUNT_MINIMUM", "VALIDITY",
        "SELECT COUNT(*) FROM fact_city",
        "SELECT CASE WHEN COUNT(*) < 20000 THEN 1 ELSE 0 END FROM fact_city",
        "Expect at least 20,000 populated places"
    ))

    results.append(run_check(engine,
        "fact_city", "NULL_COUNTRY_CODE", "COMPLETENESS",
        "SELECT COUNT(*) FROM fact_city",
        "SELECT COUNT(*) FROM fact_city WHERE country_code IS NULL"
    ))

    results.append(run_check(engine,
        "fact_city", "NEGATIVE_POPULATION", "VALIDITY",
        "SELECT COUNT(*) FROM fact_city",
        "SELECT COUNT(*) FROM fact_city WHERE population < 0"
    ))

    results.append(run_check(engine,
        "fact_city", "DUPLICATE_GEONAMEID", "UNIQUENESS",
        "SELECT COUNT(*) FROM fact_city",
        "SELECT COUNT(*) - COUNT(DISTINCT geonameid) FROM fact_city"
    ))

    # ── dim_country ───────────────────────────────────────────────────────────

    results.append(run_check(engine,
        "dim_country", "ROW_COUNT_MINIMUM", "VALIDITY",
        "SELECT COUNT(*) FROM dim_country",
        "SELECT CASE WHEN COUNT(*) < 190 THEN 1 ELSE 0 END FROM dim_country",
        "Expect at least 190 countries"
    ))

    # ── Summary ───────────────────────────────────────────────────────────────
    fails    = [r for r in results if r["status"] == "FAIL"]
    warnings = [r for r in results if r["status"] == "WARNING"]
    passed   = [r for r in results if r["status"] == "PASS"]

    logger.info("=" * 60)
    logger.info(
        f"DQ Summary: {len(results)} checks | "
        f"✅ {len(passed)} PASS | ⚠️ {len(warnings)} WARNING | ❌ {len(fails)} FAIL"
    )
    if fails:
        logger.error(f"FAILED: {[r['check'] for r in fails]}")

    return results


if __name__ == "__main__":
    run_all_checks()
