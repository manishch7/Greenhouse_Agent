# GCP Deployment Guide - Greenhouse Pipeline

Step-by-step guide to deploy the Greenhouse pipeline on Google Cloud Platform.

## Prerequisites

1. **GCP Project** with billing enabled
2. **Snowflake account** with table created
3. **OpenAI API key**
4. **gcloud CLI** installed and authenticated

## Setup Steps

### 1. Enable Required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com
```

### 2. Set Environment Variables

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export SERVICE_ACCOUNT="greenhouse-pipeline-sa"
```

### 3. Create Service Account

```bash
# Create service account
gcloud iam service-accounts create $SERVICE_ACCOUNT \
  --display-name="Greenhouse Pipeline Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

### 4. Store Secrets in Secret Manager

```bash
# OpenAI API Key
echo -n "sk-your-openai-key" | gcloud secrets create openai-api-key \
  --data-file=- \
  --replication-policy="automatic"

# Grant service account access to secret
gcloud secrets add-iam-policy-binding openai-api-key \
  --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### 5. Build and Push Docker Image

```bash
# Configure Docker authentication
gcloud auth configure-docker

# Build image
docker build -t gcr.io/$PROJECT_ID/greenhouse-pipeline:latest .

# Push to Google Container Registry
docker push gcr.io/$PROJECT_ID/greenhouse-pipeline:latest
```

### 6. Create Cloud Run Job

```bash
gcloud run jobs create greenhouse-pipeline \
  --image gcr.io/$PROJECT_ID/greenhouse-pipeline:latest \
  --region $REGION \
  --service-account $SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars "SNOWFLAKE_USER=your_username" \
  --set-env-vars "SNOWFLAKE_ACCOUNT=your_account.region" \
  --set-env-vars "SNOWFLAKE_WAREHOUSE=your_warehouse" \
  --set-env-vars "SNOWFLAKE_DATABASE=your_database" \
  --set-env-vars "SNOWFLAKE_SCHEMA=your_schema" \
  --set-env-vars "SNOWFLAKE_TABLE=GREENHOUSE_JOBS" \
  --set-env-vars "SNOWFLAKE_ROLE=your_role" \
  --set-secrets "SNOWFLAKE_PASSWORD=snowflake-password:latest" \
  --set-secrets "OPENAI_API_KEY=openai-api-key:latest" \
  --max-retries 1 \
  --task-timeout 30m \
  --memory 2Gi \
  --cpu 2
```

### 7. Test the Job Manually

```bash
# Execute the job
gcloud run jobs execute greenhouse-pipeline \
  --region $REGION \
  --wait

# View logs
gcloud run jobs executions list \
  --job greenhouse-pipeline \
  --region $REGION

# Get detailed logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=greenhouse-pipeline" \
  --limit 50 \
  --format json
```

### 8. Create Cloud Scheduler Job

```bash
# Create scheduler job to run daily at 9 AM UTC
gcloud scheduler jobs create http greenhouse-daily-run \
  --location $REGION \
  --schedule "0 9 * * *" \
  --time-zone "UTC" \
  --uri "https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/greenhouse-pipeline:run" \
  --http-method POST \
  --oauth-service-account-email $SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com
```

### 9. Verify Scheduler

```bash
# List scheduled jobs
gcloud scheduler jobs list --location $REGION

# Trigger manually for testing
gcloud scheduler jobs run greenhouse-daily-run --location $REGION

# View scheduler logs
gcloud logging read "resource.type=cloud_scheduler_job" --limit 10
```

## Configuration Options

### Adjust Schedule
```bash
# Run every 6 hours
--schedule "0 */6 * * *"

# Run Monday-Friday at 6 AM
--schedule "0 6 * * 1-5"

# Run twice daily
--schedule "0 6,18 * * *"
```

### Adjust Resources
```bash
# For larger companies list
--memory 4Gi --cpu 4

# For minimal cost
--memory 1Gi --cpu 1
```

### Adjust Timeout
```bash
# For very large runs
--task-timeout 60m

# For quick runs
--task-timeout 15m
```

## Monitoring & Alerts

### 1. View Job Executions
```bash
gcloud run jobs executions list \
  --job greenhouse-pipeline \
  --region $REGION \
  --limit 10
```

### 2. Stream Logs
```bash
gcloud logging tail "resource.type=cloud_run_job AND resource.labels.job_name=greenhouse-pipeline" \
  --format=json
```

### 3. Create Alert Policy
```bash
# Alert on job failures
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Greenhouse Pipeline Failures" \
  --condition-display-name="Job Failed" \
  --condition-filter='resource.type="cloud_run_job" AND resource.labels.job_name="greenhouse-pipeline" AND severity="ERROR"'
```

## Cost Estimation

### Cloud Run Jobs
- **Compute**: ~$0.10-0.50 per run (depending on duration)
- **Requests**: First 2M free, then $0.40/M
- **Estimated monthly**: $3-15 (running daily)

### Secret Manager
- **Secret storage**: $0.06 per secret per month
- **Access operations**: $0.03 per 10K accesses
- **Estimated monthly**: ~$0.20

### Total Estimated Cost
- **Monthly**: $5-20 (depending on job complexity and frequency)

## Troubleshooting

### Job Fails Immediately
```bash
# Check job configuration
gcloud run jobs describe greenhouse-pipeline --region $REGION

# View recent execution logs
gcloud logging read "resource.type=cloud_run_job" --limit 100
```

### Snowflake Connection Issues
- Verify credentials in Secret Manager
- Check Snowflake IP allowlist (GCP Cloud Run IPs may vary)
- Test connectivity from Cloud Shell

### OpenAI API Errors
- Verify API key in Secret Manager
- Check OpenAI usage limits
- Review rate limiting settings

### Timeout Issues
- Increase `--task-timeout`
- Reduce batch sizes in code
- Split into multiple smaller jobs

## Updating the Pipeline

### 1. Build New Image
```bash
docker build -t gcr.io/$PROJECT_ID/greenhouse-pipeline:v2 .
docker push gcr.io/$PROJECT_ID/greenhouse-pipeline:v2
```

### 2. Update Cloud Run Job
```bash
gcloud run jobs update greenhouse-pipeline \
  --image gcr.io/$PROJECT_ID/greenhouse-pipeline:v2 \
  --region $REGION
```

### 3. Test Before Scheduling
```bash
gcloud run jobs execute greenhouse-pipeline --region $REGION --wait
```

## Cleanup

### Delete Everything
```bash
# Delete scheduler
gcloud scheduler jobs delete greenhouse-daily-run --location $REGION

# Delete Cloud Run job
gcloud run jobs delete greenhouse-pipeline --region $REGION

# Delete secrets
gcloud secrets delete openai-api-key
gcloud secrets delete snowflake-password

# Delete service account
gcloud iam service-accounts delete $SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com

# Delete container images
gcloud container images delete gcr.io/$PROJECT_ID/greenhouse-pipeline:latest
```

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use Secret Manager** for all sensitive data
3. **Limit service account permissions** to minimum required
4. **Enable Cloud Audit Logs** for compliance
5. **Rotate credentials** periodically
6. **Use VPC if needed** for network isolation

## Support

For issues:
1. Check Cloud Run job logs
2. Verify Snowflake table structure
3. Test individual pipeline steps locally
4. Review OpenAI API quota
