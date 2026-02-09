FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline files
COPY snowflake_utils.py .
COPY Greenhouse_Fetch.py .
COPY Greenhouse_Title_Filtering.py .
COPY Greenhouse_Location_Filtering.py .
COPY Greenhouse_Jobs_Matcher.py .
COPY job.py .

# Copy supporting files
COPY companies.txt .
COPY Resume.pdf .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the orchestrator
CMD ["python", "job.py"]
