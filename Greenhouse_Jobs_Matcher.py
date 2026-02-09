"""
Greenhouse_Jobs_Matcher.py

Match jobs against resume using LLM.
Processes jobs where TITLE_FILTERED = 'TRUE' AND IN_USA = 'Yes' AND FIT_SCORE IS NULL.
Updates FIT_SCORE, VISA_SPONSOR, and REASON directly in Snowflake.
"""

import os
import asyncio
import logging
import time
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI
from pypdf import PdfReader

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
RESUME_PDF = "Manish_Choudhary_Resume.pdf"
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
    return pd.read_sql(
        f"""
        SELECT
            JOB_ID as job_id,
            TITLE as title,
            DESCRIPTION as description
        FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
        WHERE TITLE_FILTERED = 'TRUE'
          AND IN_USA = 'Yes'
          AND FIT_SCORE IS NULL
        """,
        engine
    )

# ---------- ASYNC MATCH ----------
async def analyze_job(job, sem, resume_text):
    async with sem:
        prompt = f"""
You are matching a job description against a candidate's resume.

Evaluate:
1. Skill, Experience and role alignment 
2. Visa eligibility

Match Evaluation:
- Compare core skills, tools, responsibilities, and seniority
- Base conclusions only on explicit evidence
- Do not assume missing requirements

Visa Rules (IMPORTANT):
Set visa = "No" ONLY IF the description explicitly states:
- U.S. citizenship is required
- Security clearance is required
- F-1 / OPT / STEM OPT candidates are not accepted

If it only says:
- "No visa sponsorship"
- "We do not sponsor visas"

Do NOT mark visa as "No". Mark as "Yes".

Resume:
{resume_text}

Job Title:
{job['title']}

Job Description:
{job['description']}

Output Format (STRICT):
score: integer between 0 and 100
visa: Yes or No
reason: 1–2 concise sentences
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
        reason = ""

        for line in text.splitlines():
            l = line.lower()
            if l.startswith("score"):
                score = line.split(":", 1)[1].strip()
            elif l.startswith("visa"):
                visa = line.split(":", 1)[1].strip().lower()
            elif l.startswith("reason"):
                reason = line.split(":", 1)[1].strip()

        if not score.isdigit():
            score = "0"
        if visa not in ("yes", "no"):
            visa = "yes"

        return (
            score,
            visa,
            reason,
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

        update_sql = f"""
            UPDATE {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
            SET
                FIT_SCORE = %s,
                VISA_SPONSOR = %s,
                REASON = %s
            WHERE JOB_ID = %s
        """

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
                # Build VALUES clause for MERGE (much faster than executemany)
                # row format: (score, visa, reason, job_id)
                values_list = [
                    f"('{job_id}', {score}, '{visa}', '{reason.replace(chr(39), chr(39)+chr(39))}')"
                    for score, visa, reason, job_id in rows
                ]
                values_str = ",\n".join(values_list)
                
                merge_sql = f"""
                MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} AS target
                USING (
                    SELECT 
                        column1 AS job_id,
                        column2 AS fit_score,
                        column3 AS visa_sponsor,
                        column4 AS reason
                    FROM VALUES {values_str}
                ) AS source
                ON target.JOB_ID = source.job_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.FIT_SCORE = source.fit_score,
                        target.VISA_SPONSOR = source.visa_sponsor,
                        target.REASON = source.reason
                """
                
                cur.execute(merge_sql)
                updated += len(rows)
                conn.commit()

            logger.info("Batch %d-%d completed in %.2f seconds (Total: %d jobs updated)", 
                       i+1, min(i+batch_size, len(jobs)), time.time() - batch_start, updated)

        logger.info("Done — updated %d jobs total (Total time: %.2f seconds)", updated, time.time() - start_time)

    finally:
        conn.close()
        engine.dispose()


# ---------- RUN ----------
if __name__ == "__main__":
    asyncio.run(run())