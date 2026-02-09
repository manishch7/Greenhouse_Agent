# Greenhouse Jobs Pipeline - Snowflake Edition

Production-ready Greenhouse job scraping and matching pipeline using Snowflake as the single source of truth.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Cloud Run Job (GCP)                       │
│                  Triggered by Cloud Scheduler                │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                        job.py                                │
│                   (Orchestrator)                             │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
    Step 1              Step 2              Step 3              Step 4
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Fetch &    │───>│    Title     │───>│   Location   │───>│  Job-Resume  │
│   Insert     │    │   Filtering  │    │ Classification│    │   Matching   │
│              │    │              │    │   (LLM)      │    │    (LLM)     │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
        │                   │                   │                   │
        └───────────────────┴───────────────────┴───────────────────┘
                                    ▼
                        ┌───────────────────────┐
                        │   Snowflake Table     │
                        │  (Single Source of    │
                        │      Truth)           │
                        └───────────────────────┘
```

## Pipeline Steps

### 1. **Greenhouse_Fetch_To_Snowflake.py**
- Fetches jobs from Greenhouse API for companies in `companies.txt`
- Async concurrent fetching (25 companies at a time)
- Only inserts NEW jobs (deduplication by JOB_ID + COMPANY)
- Filters by last N days (default: 1 day)

**Columns populated:**
- JOB_ID
- COMPANY
- TITLE
- LOCATION
- DEPARTMENT
- PUBLISHED_AT
- URL
- DESCRIPTION

### 2. **Greenhouse_Title_Filtering_Snowflake.py**
- Processes jobs where `TITLE_FILTERED IS NULL`
- Uses pandas vectorized keyword matching
- Sets `TITLE_FILTERED = 'TRUE'` or `'FALSE'`
- Direct UPDATE - no temp tables

**Logic:**
- TRUE: Contains keywords AND doesn't match exclusions
- FALSE: Everything else

### 3. **Greenhouse_Location_Filtering_Snowflake.py**
- Processes jobs where `TITLE_FILTERED = 'TRUE'` AND `IN_USA IS NULL`
- Uses OpenAI GPT-4o-mini for location classification
- Sets `IN_USA = 'Yes'` or `'No'`
- Async batch processing

**Classification Rules:**
- "Yes" if: USA, Remote, or multiple locations with ≥1 USA/Remote
- "No" if: All locations clearly outside USA

### 4. **Greenhouse_Jobs_Matcher_Snowflake.py**
- Processes jobs where `TITLE_FILTERED = 'TRUE'` AND `IN_USA = 'Yes'` AND `FIT_SCORE IS NULL`
- Loads resume PDF and matches against job descriptions
- Uses OpenAI GPT-4o-mini for matching
- Updates `FIT_SCORE`, `VISA_SPONSOR`, `REASON`

**Outputs:**
- FIT_SCORE: 0-100 (skill/experience alignment)
- VISA_SPONSOR: Yes/No (strict rules for citizenship requirements)
- REASON: 1-2 sentence explanation

## Snowflake Table Schema

```sql
CREATE TABLE GREENHOUSE_JOBS (
    JOB_ID VARCHAR,
    COMPANY VARCHAR,
    TITLE VARCHAR,
    LOCATION VARCHAR,
    DEPARTMENT VARCHAR,
    PUBLISHED_AT TIMESTAMP_TZ,
    URL VARCHAR,
    DESCRIPTION TEXT,
    
    -- Filtering columns
    TITLE_FILTERED VARCHAR,  -- 'TRUE' | 'FALSE' | NULL
    IN_USA VARCHAR,           -- 'Yes' | 'No' | NULL
    
    -- Matching columns
    FIT_SCORE INTEGER,        -- 0-100 | NULL
    VISA_SPONSOR VARCHAR,     -- 'yes' | 'no' | NULL
    REASON TEXT,              -- Explanation | NULL
    
    PRIMARY KEY (JOB_ID, COMPANY)
);
```

## Setup

### 1. Environment Variables
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Companies File
Create `companies.txt`:
```
anthropic
openai
google
# Add one company per line
```

### 3. Resume File
Place your resume as `Resume.pdf` in the project root.

### 4. Snowflake Table
Create the table using the schema above.

## Running Locally

### Test Individual Steps
```bash
# Step 1: Fetch jobs
python Greenhouse_Fetch_To_Snowflake.py

# Step 2: Title filter
python Greenhouse_Title_Filtering_Snowflake.py

# Step 3: Location filter
python Greenhouse_Location_Filtering_Snowflake.py

# Step 4: Job matcher
python Greenhouse_Jobs_Matcher_Snowflake.py
```

### Run Full Pipeline
```bash
python job.py
```

## Deployment (GCP Cloud Run)

### 1. Build Docker Image
```bash
docker build -t gcr.io/YOUR_PROJECT/greenhouse-pipeline .
docker push gcr.io/YOUR_PROJECT/greenhouse-pipeline
```

### 2. Deploy to Cloud Run Jobs
```bash
gcloud run jobs create greenhouse-pipeline \
  --image gcr.io/YOUR_PROJECT/greenhouse-pipeline \
  --region us-central1 \
  --set-env-vars SNOWFLAKE_USER=xxx,SNOWFLAKE_PASSWORD=xxx,... \
  --set-secrets OPENAI_API_KEY=openai-key:latest \
  --max-retries 1 \
  --task-timeout 30m
```

### 3. Schedule with Cloud Scheduler
```bash
gcloud scheduler jobs create http greenhouse-daily \
  --location us-central1 \
  --schedule "0 9 * * *" \
  --uri "https://YOUR_REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/YOUR_PROJECT/jobs/greenhouse-pipeline:run" \
  --http-method POST \
  --oauth-service-account-email YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com
```

## Key Design Patterns

### ✅ Idempotent Processing
- Each step only processes records with NULL values in target columns
- Safe to re-run at any time
- No duplicate inserts

### ✅ Incremental Updates
- Only processes jobs from last 2 days
- Reduces computational cost
- Avoids reprocessing old data

### ✅ No Staging Tables
- All updates are direct `executemany()` operations
- Simpler architecture
- Single source of truth

### ✅ Async Where It Matters
- LLM calls use `asyncio.gather()` for parallelism
- Configurable concurrency limits
- Batch processing for efficiency

### ✅ Error Resilience
- Try/finally blocks ensure connection cleanup
- Graceful handling of API failures
- Logging at each step

## Configuration

### Keyword Filtering
Edit `KEYWORDS` and `TITLE_EXCLUDE` in `Greenhouse_Title_Filtering_Snowflake.py`:

```python
KEYWORDS = [
    "sql", "python", "snowflake", "analyst", 
    "data engineer", "data scientist", "etl"
]

TITLE_EXCLUDE = [
    "staff", "principal", "architect", 
    "manager", "intern", "devops"
]
```

### LLM Settings
Models used:
- **Location filtering**: `gpt-4o-mini` (fast, cheap)
- **Job matching**: `gpt-4o-mini` (balanced quality/cost)

Adjust in respective files if needed.

### Batch Sizes
- **Fetch concurrency**: 25 companies
- **LLM concurrency**: 5 jobs (location & matching)
- **Batch size**: 20 jobs per commit

## Cost Optimization

### OpenAI API
- Location filtering: ~100 tokens/job
- Job matching: ~1000 tokens/job
- Estimated: $0.01-0.02 per 100 jobs

### Snowflake
- Minimal compute (few seconds per run)
- Storage cost depends on retention policy

### Recommendations
- Run daily during off-peak hours
- Adjust `LOOKBACK_DAYS` based on job posting velocity
- Monitor OpenAI token usage

## Monitoring

Check logs for:
```
✅ New jobs added to Snowflake
✅ TITLE_FILTERED updated successfully
✅ IN_USA updated successfully
✅ Updated X jobs total (FIT_SCORE)
```

## Troubleshooting

### No jobs inserted
- Check `companies.txt` exists
- Verify company names match Greenhouse board names
- Increase `LOOKBACK_DAYS`

### Location/matching not running
- Ensure previous step completed (`TITLE_FILTERED = 'TRUE'`)
- Check OpenAI API key is set
- Verify `Resume.pdf` exists

### Snowflake connection errors
- Validate credentials in `.env`
- Ensure warehouse is running
- Check network/firewall rules

## License
MIT
