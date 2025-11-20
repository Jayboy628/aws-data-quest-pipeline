# Pseudocode / structure inside glue_job.py

args = getResolvedOptions(sys.argv, ["JOB_NAME", "LANDING_BUCKET", "PROCESSED_BUCKET", "ANALYTICS_BUCKET", "ENV"])

# 1. Identify latest ingest_dt for BLS + population (e.g., via S3 prefix listing or pass as argument)
# 2. Read BLS text files into Spark DataFrame (schema: series_id, year, period, value)
# 3. Read population JSON into DataFrame (year, population)
# 4. Build:
#    - time_series_pr
#    - population_us
# 5. Compute:
#    - report_best_year (sum value per series_id/year, pick max year per series)
#    - report_joined (join time_series_pr with population_us on year)
# 6. Write Parquet to PROCESSED:
#    - processed/bls/time_series_pr/year=YYYY/...
#    - processed/population/population_us/year=YYYY/...
# 7. Write CSVs to ANALYTICS:
#    - analytics/reports/run_date=YYYY-MM-DD/report_best_year.csv
#    - analytics/reports/run_date=YYYY-MM-DD/report_joined.csv
