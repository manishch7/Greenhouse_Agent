"""
Greenhouse_Title_Filtering.py

Uses pandas for vectorized logic and a single batched UPDATE
to update TITLE_FILTERED ('TRUE' / 'FALSE') in-place in Snowflake.
No temp tables. No overwrite.
"""

# ---------- STANDARD LIBS ----------
import os
import logging
import warnings
import time

# ---------- THIRD PARTY ----------
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ---------- LOCAL ----------
from snowflake_utils import get_connection, get_engine, SF_DATABASE, SF_SCHEMA, SF_TABLE

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=UserWarning)

# ---------- ENV ----------
load_dotenv()

# ---------- FILTER CONFIG ----------
KEYWORDS = [
    "sql", "python", "snowflake", "analyst", "data pipeline",
    "data engineer", "data scientist", "big data", "etl",
    "powerbi", "power bi", "tableau", "n8n", "automation", "airflow"
]

TITLE_EXCLUDE = [
    "staff", "principal", "architect", "lead", "director",
    "manager", "intern", "co-op", "sre", "devops",
    "security", "platform engineer", "frontend",
    "front end", "full stack", "ios", "android",
    "mobile", "java", "cloud", "writer", "testing",
    "sales engineer", "pre-sales"
]

# ---------- PIPELINE ENTRYPOINT ----------
def run():
    """
    Update TITLE_FILTERED in-place in Snowflake using vectorized pandas logic.
    """

    engine = get_engine()
    conn = get_connection()
    
    try:
        start_time = time.time()
        logger.info("Fetching jobs to process")

        df = pd.read_sql(
            text(f"""
            SELECT 
                JOB_ID as job_id, 
                TITLE as title, 
                DESCRIPTION as description
            FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
            WHERE TITLE_FILTERED IS NULL
              AND PUBLISHED_AT >= DATEADD(day, -2, CURRENT_TIMESTAMP())
            """),
            engine  # ✅ Use engine for reads
        )
        
        logger.info("Fetch completed in %.2f seconds", time.time() - start_time)

        if df.empty:
            logger.info("No jobs to process")
            return

        logger.info("Processing %d jobs in pandas", len(df))
        
        process_start = time.time()

        exclude_mask = df["title"].fillna("").str.contains(
            "|".join(TITLE_EXCLUDE),
            case=False,
            regex=True,
            na=False
        )

        keyword_mask = df["description"].fillna("").str.contains(
            "|".join(KEYWORDS),
            case=False,
            regex=True,
            na=False
        )

        df["TITLE_FILTERED"] = (
            (~exclude_mask & keyword_mask)
            .map({True: "TRUE", False: "FALSE"})
        )
        
        logger.info("Processing completed in %.2f seconds", time.time() - process_start)

        update_rows = list(
            zip(
                df["TITLE_FILTERED"].tolist(),
                df["job_id"].tolist()
            )
        )

        logger.info("Updating %d rows in Snowflake", len(update_rows))
        
        update_start = time.time()

        # Build VALUES clause for MERGE (much faster than executemany)
        values_list = [f"('{job_id}', '{filtered}')" for filtered, job_id in update_rows]
        values_str = ",\n".join(values_list)
        
        merge_sql = f"""
        MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} AS target
        USING (
            SELECT column1 AS job_id, column2 AS title_filtered
            FROM VALUES {values_str}
        ) AS source
        ON target.JOB_ID = source.job_id
        WHEN MATCHED THEN
            UPDATE SET target.TITLE_FILTERED = source.title_filtered
        """

        with conn.cursor() as cur:
            cur.execute(merge_sql)

        conn.commit()
        
        logger.info("Update completed in %.2f seconds", time.time() - update_start)
        logger.info("Done — TITLE_FILTERED updated in-place (Total: %.2f seconds)", time.time() - start_time)

    finally:
        conn.close()
        engine.dispose()


# ---------- RUN ----------
if __name__ == "__main__":
    run()