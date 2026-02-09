"""
Greenhouse_Fetch.py

Fetch recent jobs from Greenhouse public Job Board API
and INSERT only NEW jobs into Snowflake.
"""

# ---------- STANDARD LIBS ----------
import os
import html
import re
import asyncio
import aiohttp
import ssl
import certifi
import logging
from datetime import datetime, timedelta, timezone
from aiohttp import ClientResponseError

# ---------- THIRD PARTY ----------
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

# ---------- LOCAL ----------
from snowflake_utils import get_connection, SF_DATABASE, SF_SCHEMA, SF_TABLE

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- ENV ----------
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# ---------- CONFIG ----------
API_TEMPLATE = "https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true"

DEFAULT_COMPANIES_FILE = "Companies.txt"
DEFAULT_LOOKBACK_DAYS = 1

FETCH_CONCURRENCY = 25
FETCH_BATCH_SIZE = 400


# ---------- LOAD RECENT KEYS ----------
def load_recent_keys(conn):
    cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT JOB_ID, COMPANY
            FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
            WHERE PUBLISHED_AT >= DATEADD(day, -2, CURRENT_TIMESTAMP())
        """)
        keys = set(cur.fetchall())
        logger.info("Loaded %d recent keys from Snowflake", len(keys))
        return keys
    finally:
        cur.close()


# ---------- HELPERS ----------
def load_companies(path: str) -> list[str]:
    if not os.path.exists(path) and os.path.exists(path.lower()):
        path = path.lower()
    if not os.path.exists(path):
        return []

    companies = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                companies.append(line.split("#", 1)[0].strip())
    return companies


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = html.unescape(raw)
    text = text.replace("\xa0", " ").replace("&nbsp;", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_iso_dt(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def normalize_job(company: str, job: dict) -> dict:
    published_at = job.get("first_published") or job.get("updated_at") or ""
    job_url = job.get("absolute_url") or ""
    job_id = job.get("id") or f"{company}:{job_url}:{published_at}:{job.get('title','')}"
    
    location = job.get("location", {})
    location_name = location.get("name", "") if isinstance(location, dict) else ""
    
    departments = job.get("departments", [])
    department_name = departments[0].get("name", "") if departments and isinstance(departments, list) else ""

    return {
        "JOB_ID": str(job_id).strip(),
        "COMPANY": company,
        "TITLE": (job.get("title") or "").strip(),
        "LOCATION": location_name.strip(),
        "DEPARTMENT": department_name.strip(),
        "PUBLISHED_AT": published_at,
        "URL": job_url,
        "DESCRIPTION": clean_html(job.get("content", "")),
    }


# ---------- ASYNC FETCH ----------
async def fetch_company_jobs(session, company: str):
    url = API_TEMPLATE.format(company=company)
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()
            jobs = data.get("jobs", []) if isinstance(data, dict) else []
            return company, jobs

    except ClientResponseError as e:
        logger.warning("HTTP %s for company=%s", e.status, company)
        return company, []

    except Exception:
        logger.exception("Unexpected error fetching company=%s", company)
        return company, []


async def fetch_all(companies: list[str]):
    timeout = aiohttp.ClientTimeout(sock_connect=15, sock_read=30)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    connector = aiohttp.TCPConnector(limit=FETCH_CONCURRENCY, ssl=ssl_context)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        results = []
        for i in range(0, len(companies), FETCH_BATCH_SIZE):
            batch = companies[i:i + FETCH_BATCH_SIZE]
            results.extend(await asyncio.gather(
                *(fetch_company_jobs(session, c) for c in batch)
            ))
            await asyncio.sleep(0.25)
        return results


# ---------- PIPELINE ENTRYPOINT ----------
def run(
    days: float = DEFAULT_LOOKBACK_DAYS,
    companies_file: str = DEFAULT_COMPANIES_FILE,
    companies_override: list[str] | None = None,
):
    """
    Fetch recent Greenhouse jobs and insert only NEW jobs into Snowflake.

    Args:
        days: Lookback window in days
        companies_file: Path to companies file
        companies_override: Optional explicit list of companies
    """

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    conn = get_connection()
    try:
        existing_keys = load_recent_keys(conn)

        companies = companies_override or load_companies(companies_file)
        if not companies:
            logger.error("No companies found")
            return

        logger.info("Fetching %d companies", len(companies))
        results = asyncio.run(fetch_all(companies))

        rows = []
        for company, jobs in results:
            for j in jobs:
                dt = parse_iso_dt(j.get("first_published") or j.get("updated_at"))
                if not dt or dt < cutoff:
                    continue

                job = normalize_job(company, j)
                key = (job["JOB_ID"], job["COMPANY"])

                if key not in existing_keys:
                    rows.append(job)
                    existing_keys.add(key)

        if not rows:
            logger.info("No new jobs to insert")
            return

        df = pd.DataFrame(rows)
        logger.info("Inserting %d new jobs", len(df))

        write_pandas(
            conn,
            df,
            table_name=SF_TABLE,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            auto_create_table=False,
            chunk_size=500,
            parallel=1,
        )

        logger.info("Done — only new jobs inserted")

    finally:
        conn.close()


# ---------- RUN ----------
if __name__ == "__main__":
    run()
