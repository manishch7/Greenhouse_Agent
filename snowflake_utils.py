"""
snowflake_utils.py

Centralized Snowflake connection management for Greenhouse pipeline.
"""

import os
import snowflake.connector
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
load_dotenv()

# ---------- SNOWFLAKE CONFIG ----------
SF_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SF_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]
SF_TABLE = os.environ["SNOWFLAKE_TABLE"]

SF_USER = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SF_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SF_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SF_ROLE = os.environ.get("SNOWFLAKE_ROLE")


# ---------- CONNECTION HELPERS ----------
def get_engine():
    """Get SQLAlchemy engine for pandas operations."""
    url = URL.create(
        "snowflake",
        username=SF_USER,
        password=SF_PASSWORD,
        host=SF_ACCOUNT,
        database=SF_DATABASE,
        query={
            "schema": SF_SCHEMA,
            "warehouse": SF_WAREHOUSE,
            **({"role": SF_ROLE} if SF_ROLE else {})
        }
    )
    return create_engine(url)


def get_connection():
    """Get raw Snowflake connection for write operations."""
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=SF_ROLE
    )