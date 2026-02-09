"""
Greenhouse_Location_Filtering.py

Populate IN_USA column using LLM-based location classification.
Processes only TITLE_FILTERED = 'TRUE' jobs with IN_USA IS NULL,
LOCATION IS NOT NULL, from the last 2 days.

UPDATED VERSION:
✓ Writes directly into main table
✓ No temp/staging tables created
"""

# ---------- STANDARD LIBS ----------
import os
import asyncio
import logging
import warnings
import time

# ---------- THIRD PARTY ----------
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ---------- LOCAL ----------
from snowflake_utils import get_engine, get_connection, SF_DATABASE, SF_SCHEMA, SF_TABLE

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=UserWarning)

# ---------- ENV ----------
load_dotenv()
client = AsyncOpenAI()

# ---------- LLM LOCATION CLASSIFIER ----------
async def is_in_usa(location_text: str) -> str:
    if not location_text or not str(location_text).strip():
        return "No"

    prompt = f"""
Location: "{location_text}"

Rules:
- Answer "Yes" if:
  - Job is in the USA
  - Job is Remote
  - Job lists multiple locations AND at least one is in the USA or Remote
- Answer "No" ONLY if:
  - All locations are clearly outside the USA

Answer with exactly one word: Yes or No
"""

    response = await client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    answer = response.choices[0].message.content.strip().lower()
    return "Yes" if answer.startswith("yes") else "No"

# ---------- PIPELINE ENTRYPOINT ----------
async def run():
    engine = get_engine()
    conn = get_connection()

    try:
        start_time = time.time()
        logger.info("Loading jobs to classify location")

        df = pd.read_sql(
            f"""
            SELECT
                JOB_ID   AS job_id,
                LOCATION AS location
            FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
            WHERE TITLE_FILTERED = 'TRUE'
              AND IN_USA IS NULL
              AND LOCATION IS NOT NULL
              AND PUBLISHED_AT >= DATEADD(day, -2, CURRENT_TIMESTAMP())
            """,
            engine
        )
        
        logger.info("Fetch completed in %.2f seconds", time.time() - start_time)

        if df.empty:
            logger.info("No jobs to process")
            return

        logger.info("Classifying %d job locations", len(df))
        
        classify_start = time.time()

        tasks = [is_in_usa(loc) for loc in df["location"].astype(str)]
        df["in_usa"] = await asyncio.gather(*tasks)
        
        logger.info("Classification completed in %.2f seconds", time.time() - classify_start)
        logger.info("Updating Snowflake table directly")
        
        update_start = time.time()

        # Build VALUES clause for MERGE (much faster than executemany)
        values_list = [f"('{job_id}', '{in_usa}')" for job_id, in_usa in zip(df["job_id"], df["in_usa"])]
        values_str = ",\n".join(values_list)
        
        merge_sql = f"""
        MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} AS target
        USING (
            SELECT column1 AS job_id, column2 AS in_usa
            FROM VALUES {values_str}
        ) AS source
        ON target.JOB_ID = source.job_id
        WHEN MATCHED THEN
            UPDATE SET target.IN_USA = source.in_usa
        """

        with conn.cursor() as cur:
            cur.execute(merge_sql)

        conn.commit()
        
        logger.info("Update completed in %.2f seconds", time.time() - update_start)
        logger.info("Done — IN_USA updated successfully (Total: %.2f seconds)", time.time() - start_time)

    finally:
        conn.close()
        engine.dispose()


# ---------- RUN ----------
if __name__ == "__main__":
    asyncio.run(run())