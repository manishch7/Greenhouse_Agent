"""
job.py

Single orchestrator for the Greenhouse → Snowflake pipeline.
Designed for Cloud Run Jobs + Cloud Scheduler.
"""

import asyncio
import logging

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- PIPELINE STEPS ----------
from Greenhouse_Fetch import run as scrape_greenhouse
from Greenhouse_Title_Filtering import run as title_filter
from Greenhouse_Location_Filtering import run as location_filter
from Greenhouse_Jobs_Matcher import run as matcher


def main():
    logger.info("🚀 Starting Greenhouse pipeline")

    # 1️⃣ Fetch & insert new jobs
    logger.info("Step 1: Scraping Greenhouse jobs")
    scrape_greenhouse(days=1)

    # 2️⃣ Title-based filtering
    logger.info("Step 2: Running title filter")
    title_filter()

    # 3️⃣ LLM-based location classification (async)
    logger.info("Step 3: Classifying job locations (LLM)")
    asyncio.run(location_filter())

    # 4️⃣ Resume → Job matching (async)
    logger.info("Step 4: Computing FIT_SCORE (LLM)")
    asyncio.run(matcher())

    logger.info("✅ Greenhouse pipeline completed successfully")


if __name__ == "__main__":
    main()
