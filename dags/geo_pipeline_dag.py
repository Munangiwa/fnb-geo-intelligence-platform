"""
geo_pipeline_dag.py
-------------------
AIRFLOW DAG : FNB Geo Intelligence Pipeline
-------------------------------------------
Direct equivalent of a Control-M job chain.

CONTROL-M MAPPING:
  Airflow DAG          ↔  Control-M Job Folder
  Airflow Task         ↔  Control-M Job
  Task dependencies    ↔  Control-M IN/OUT conditions
  schedule (cron)      ↔  Control-M time-based scheduling
  on_failure_callback  ↔  Control-M SHOUT/notification
  retries              ↔  Control-M retry settings

SCHEDULE: Daily at 06:00 SAST (04:00 UTC)
          OurAirports updates nightly — we run after midnight to get freshest data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow.utils.dates      import days_ago

default_args = {
    "owner"            : "data_engineering",
    "depends_on_past"  : False,
    "email"            : ["data-alerts@fnb.co.za"],
    "email_on_failure" : True,
    "email_on_retry"   : False,
    "retries"          : 2,
    "retry_delay"      : timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id            = "fnb_geo_intelligence_pipeline",
    default_args      = default_args,
    description       = "Daily geo pipeline: OurAirports + GeoNames → SQLite EDW",
    schedule_interval = "0 4 * * *",   # 04:00 UTC = 06:00 SAST daily
    start_date        = days_ago(1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["geo", "edw", "daily", "fnb"],
) as dag:

    start = EmptyOperator(task_id="pipeline_start")

    def task_test_connection():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from pipeline.db_connection import test_connection
        if not test_connection():
            raise ConnectionError("Database connection failed")

    check_db = PythonOperator(
        task_id="check_db_connection",
        python_callable=task_test_connection
    )

    def task_extract_airports(**context):
        import sys
        sys.path.insert(0, "/opt/airflow")
        from pipeline.extract.extract_airports import extract_airports, extract_countries
        airports  = extract_airports()
        countries = extract_countries()
        context["ti"].xcom_push(key="airport_count",  value=len(airports))
        context["ti"].xcom_push(key="country_count",  value=len(countries))

    extract_airports_task = PythonOperator(
        task_id="extract_airports",
        python_callable=task_extract_airports,
        provide_context=True
    )

    def task_extract_cities(**context):
        import sys
        sys.path.insert(0, "/opt/airflow")
        from pipeline.extract.extract_cities import extract_cities, extract_country_info
        cities      = extract_cities()
        countryinfo = extract_country_info()
        context["ti"].xcom_push(key="city_count",  value=len(cities))
        context["ti"].xcom_push(key="cinfo_count", value=len(countryinfo))

    extract_cities_task = PythonOperator(
        task_id="extract_cities",
        python_callable=task_extract_cities,
        provide_context=True
    )

    def task_run_pipeline():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from run_pipeline import run_full_pipeline
        success = run_full_pipeline()
        if not success:
            raise ValueError("Pipeline completed with DQ FAILURES — check dq_results table")

    run_pipeline_task = PythonOperator(
        task_id="transform_load_dq",
        python_callable=task_run_pipeline,
        execution_timeout=timedelta(hours=1)
    )

    def task_dq_gate():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from pipeline.db_connection import get_engine
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            failures = conn.execute(text("""
                SELECT COUNT(*) FROM dq_results
                WHERE run_date = date('now') AND status = 'FAIL'
            """)).scalar()
        if failures > 0:
            raise ValueError(f"{failures} DQ checks FAILED — pipeline halted at QA gate")

    dq_gate = PythonOperator(
        task_id="dq_quality_gate",
        python_callable=task_dq_gate
    )

    end = EmptyOperator(task_id="pipeline_complete")

    # ── Dependencies — equivalent to Control-M IN/OUT conditions ──────────────
    #
    #  start → check_db → [extract_airports, extract_cities] → run_pipeline → dq_gate → end
    #
    start >> check_db
    check_db >> [extract_airports_task, extract_cities_task]
    [extract_airports_task, extract_cities_task] >> run_pipeline_task
    run_pipeline_task >> dq_gate >> end
