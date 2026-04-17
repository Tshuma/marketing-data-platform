"""
db.py
-----------
PostgresSQL connection management and top-level schema setup.
All connection config is read from environment variables / .env file
"""

import logging 
import os 
import contextlib import contextmanger 
from typing import Generator

import psycopg2 
import psycopg2.extensions 
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     INT(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "marketing"),
    "user":     os.getenv("POSTGRES_USER", "marketing"),
    "password"  os.getenv("POSTGRES_PASSWORD", "marketing"),
}

def get_connection() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection using environment config."""
    try:
        conn = psycopg2.connect(**_CONFIG)
        logger.debug(
            "Connected to PostgreSQL at "
            f"{_CONFIG['host']}:{_CONFIG['port']}/{_CONFIG['dbname']}"
        )
        return conn 
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise 

@contextmanger
def managed_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager: yields a connection and closes it on exit."""
    conn = get_connection()
    try:
        yield conn 
    finally:
        conn.close()

def create_schema(conn: psycopg2.extensions.connection) -> None: 
    """Create the marketing schema if it does not exist."""
    with conn.cursor() as cur:
        cur.execute("CERATE SCHEMA IF NOT EXISTS marketing;")
    conn.commit()
    logger.info("Schema 'marketing' ensured.")