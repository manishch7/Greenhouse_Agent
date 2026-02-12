"""
Greenhouse_Jobs_Matcher.py

Match jobs against resume using LLM.
Processes jobs where TITLE_FILTERED = 'TRUE' AND IN_USA = 'Yes' AND FIT_SCORE IS NULL.
Updates FIT_SCORE and VISA_SPONSOR directly in Snowflake.
"""

import os
import asyncio
import logging
import time
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI
from pypdf import PdfReader
from sqlalchemy import text

# ---------- LOCAL ----------
from snowflake_utils import get_engine, get_connection, SF_DATABASE, SF_SCHEMA, SF_TABLE

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- ENV ----------
load_dotenv()

# ---------- CONFIG ----------
RESUME_PDF = "Resume.pdf"
CONCURRENCY_LIMIT = 5
BATCH_SIZE = 20

# ---------- OPENAI ----------
client = AsyncOpenAI()

# ---------- RESUME ----------
def load_resume_text(path):
    reader = PdfReader(path)
    text = ""
    for page in reader.pages:
        t = page.extract_text()
        if t:
            text += t + "\n"
    return text.strip()

# ---------- LOAD JOBS ----------
def load_jobs(engine):
    try:
        logger.info(f"Loading jobs from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")
        
        query = text(f"""
            SELECT
                JOB_ID as job_id,
                TITLE as title,
                DESCRIPTION as description
            FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
            WHERE TITLE_FILTERED = 'TRUE'
              AND IN_USA = 'Yes'
              AND FIT_SCORE IS NULL
            """)
        
        df = pd.read_sql(query, engine)
        
        # Deduplicate immediately after fetch (defense in depth)
        df = df.drop_duplicates(subset=['job_id'], keep='last')
        
        logger.info(f"Loaded {len(df)} jobs")
        return df
    except Exception as e:
        logger.error(f"Failed to load jobs: {e}", exc_info=True)
        raise

# ---------- ASYNC MATCH ----------
async def analyze_job(job, sem, resume_text):
    async with sem:
        prompt = f"""
Match this job against the candidate's resume and provide a fit score.

CANDIDATE PROFILE:
- Senior Data Analyst with 3+ years experience in SQL analytics, Python ETL, Power BI, and cloud data platforms
- Strong technical background: Snowflake, Apache Airflow, AWS/GCP, API automation, CI/CD
- Open to: Data Analyst, Data Engineer, Business Intelligence, Analytics Engineer, Data Scientist roles
- Also considering: Software Engineer roles with data/backend/automation focus

SCORING CRITERIA:
1. Technical Skills Match (40 points):
   - SQL, Python (pandas, numpy, asyncio)
   - Cloud platforms (Snowflake, AWS, GCP)
   - BI tools (Power BI, Tableau, Looker)
   - ETL pipelines, APIs, data orchestration
   
2. Experience & Domain (30 points):
   - Data analysis, reporting, dashboards
   - Data engineering, pipeline automation
   - 3+ years relevant experience
   - Analytics, compliance, operations background

3. Role Alignment (30 points):
   - Data Analyst/Engineer/BI → Excellent fit
   - Analytics Engineer/Data Scientist → Good fit
   - Software Engineer (backend/data focus) → Good fit
   - Software Engineer (frontend/mobile) → Lower fit

SCORING GUIDELINES:
- 80-100: Excellent match - Core data roles (Analyst, Engineer, BI, Analytics Engineer)
- 65-79: Good match - Data Scientist, Software Engineer with data/backend, adjacent analytics roles
- 50-64: Moderate match - Software roles with some data components, different tech stack but transferable
- 30-49: Weak match - Senior-only (7+ years), pure frontend/mobile, minimal data focus
- 0-29: Poor match - Unrelated field, 10+ years required, completely different tech

IMPORTANT: 
- Prioritize data-focused roles but don't penalize Software Engineer titles if responsibilities involve databases, APIs, pipelines, or backend systems
- Consider job descriptions, not just titles
- Missing 1-2 specific tools is fine if core skills align

VISA RULES (STRICT):
Set visa = "No" ONLY IF the description explicitly states:
- U.S. citizenship is required
- Security clearance is required
- F-1 / OPT / STEM OPT candidates are not accepted

If it only says:
- "No visa sponsorship"
- "We do not sponsor visas"

Do NOT mark visa as "No". Mark as "Yes".

RESUME:
{resume_text}

JOB DESCRIPTION:
{job['description']}

OUTPUT (exact format):
score: [0-100]
visa: [Yes/No]
"""

        try:
            resp = await client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
        except Exception as e:
            logger.error("OpenAI error: %s", e)
            return None

        text = resp.choices[0].message.content.strip()

        score = "0"
        visa = "yes"

        for line in text.splitlines():
            l = line.lower()
            if l.startswith("score"):
                score = line.split(":", 1)[1].strip()
            elif l.startswith("visa"):
                visa = line.split(":", 1)[1].strip().lower()

        if not score.isdigit():
            score = "0"
        if visa not in ("yes", "no"):
            visa = "yes"

        return (
            score,
            visa,
            job["job_id"]
        )

# ---------- PIPELINE ENTRYPOINT ----------
async def run(
    resume_pdf: str = RESUME_PDF,
    concurrency: int = CONCURRENCY_LIMIT,
    batch_size: int = BATCH_SIZE,
):
    start_time = time.time()
    logger.info("Loading resume from %s", resume_pdf)
    resume_text = load_resume_text(resume_pdf)

    engine = get_engine()
    conn = get_connection()

    try:
        fetch_start = time.time()
        df = load_jobs(engine)
        logger.info("Fetch completed in %.2f seconds", time.time() - fetch_start)
        
        if df.empty:
            logger.info("No FIT_SCORE NULL jobs to process")
            return

        jobs = df.to_dict("records")
        sem = asyncio.Semaphore(concurrency)

        cur = conn.cursor()
        updated = 0

        logger.info("Processing %d jobs in batches of %d", len(jobs), batch_size)

        for i in range(0, len(jobs), batch_size):
            batch_start = time.time()
            batch = jobs[i:i + batch_size]
            results = await asyncio.gather(
                *(analyze_job(job, sem, resume_text) for job in batch)
            )

            rows = [r for r in results if r]
            if rows:
                # Deduplicate by job_id (keep last)
                seen = set()
                unique_rows = []
                for row in reversed(rows):
                    job_id = row[2]
                    if job_id not in seen:
                        seen.add(job_id)
                        unique_rows.append(row)
                rows = list(reversed(unique_rows))
                
                # Build VALUES clause for MERGE (much faster than executemany)
                # row format: (score, visa, job_id)
                values_list = [
                    f"('{job_id}', {score}, '{visa}')"
                    for score, visa, job_id in rows
                ]
                values_str = ",\n".join(values_list)
                
                merge_sql = f"""
                MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} AS target
                USING (
                    SELECT 
                        column1 AS job_id,
                        column2 AS fit_score,
                        column3 AS visa_sponsor
                    FROM VALUES {values_str}
                ) AS source
                ON target.JOB_ID = source.job_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.FIT_SCORE = source.fit_score,
                        target.VISA_SPONSOR = source.visa_sponsor
                """
                
                cur.execute(merge_sql)
                updated += len(rows)
                conn.commit()

            logger.info("Batch %d-%d completed in %.2f seconds (Total: %d jobs updated)", 
                       i+1, min(i+batch_size, len(jobs)), time.time() - batch_start, updated)

        logger.info("Done – updated %d jobs total (Total time: %.2f seconds)", updated, time.time() - start_time)

    finally:
        conn.close()
        engine.dispose()


# ---------- RUN ----------
if __name__ == "__main__":
    asyncio.run(run())