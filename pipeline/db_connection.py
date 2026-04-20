"""
db_connection.py
----------------
Centralised database connection using SQLite.
SQLite requires zero installation — the database is a single file: geo_data.db
In production this connection string would point to Teradata or PostgreSQL.
Swapping databases = changing one line here. Nothing else in the pipeline changes.
"""

import os
from sqlalchemy import create_engine, text
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Database file sits at project root
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "geo_data.db")


def get_engine():
    """
    Returns a SQLAlchemy engine connected to SQLite.
    Production equivalent: swap this connection string for Teradata/PostgreSQL.
    """
    abs_path = os.path.abspath(DB_PATH)
    engine = create_engine(
        f"sqlite:///{abs_path}",
        echo=False,
        connect_args={"check_same_thread": False}
    )
    logger.info(f"SQLite engine → {abs_path}")
    return engine


def test_connection():
    """Health check — called at pipeline start to fail fast if DB is unreachable."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("Database connection OK")
        return True
    except Exception as e:
        logger.error(f"Database connection FAILED: {e}")
        return False
