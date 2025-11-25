# AWS Data Quest Pipeline

This project implements a production-style data pipeline for the Data Quest assignment using AWS-native services, Infrastructure as Code, and automated deployment via GitHub Actions.

## Architecture Overview

The pipeline:

1. **Ingests public datasets**:
   - BLS Productivity time-series files from  
     `https://download.bls.gov/pub/time.series/pr/`
   - US population data from the DataUSA API.

2. **Stores raw data** in an S3 landing bucket:
   - `s3://dq-landing-<account>-<env>/landing/bls/pr/ingest_dt=YYYY/MM/DD/`
   - `s3://dq-landing-<account>-<env>/landing/population/ingest_dt=YYYY/MM/DD/`

3. **Tracks ingestions** in a DynamoDB manifest table:
   - `dq_manifest_<env>` with one row per ingested object.

4. **Transforms data** with an AWS Glue job (`glue_job.py`) into:
   - `time_series_pr` (BLS)
   - `population_us` (population API JSON)

5. **Produces analytics reports**:
   - `report_best_year.csv`
   - `report_joined.csv`
   stored under  
   `s3://dq-analytics-<account>-<env>/analytics/reports/run_date=YYYY-MM-DD/`.

6. **Orchestrates everything** via:
   - AWS Step Functions (`dq-state-machine-<env>`)
   - EventBridge daily cron rule
   - SNS alerts (`dq-pipeline-alerts-<env>`)
   - Optional SQS queue for pipeline events.

## Repo Structure

```text
infra/
  01_s3.yml           # S3 buckets (landing, processed, analytics, code)
  02_dynamodb.yml     # DynamoDB manifest table
  03_secrets.yml      # Secrets Manager config (BLS User-Agent, etc.)
  04_iam.yml          # IAM roles for Lambda, Glue, Step Functions
  05_lambda.yml       # Lambda functions (BLS sync, Population API)
  06_glue.yml         # Glue ETL job definition
  07_stepfunctions.yml# Step Functions state machine
  08_eventbridge.yml  # EventBridge schedule
  deploy_pipeline.sh  # Local infra deploy helper

src/
  ingestion/
    bls_sync_lambda/
      handler.py      # Scrape BLS, write to S3, log to DynamoDB
    population_api_lambda/
      handler.py      # Call population API, write to S3, log to DynamoDB
  analytics/
    glue_job.py       # PySpark Glue job for processing + reports

.github/workflows/
  deploy.yml          # Infra deploy (CloudFormation) via GitHub Actions
  deploy_pipeline.yml # Code deploy (Lambda zips + Glue script)
