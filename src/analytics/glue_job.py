import sys
import os
import json
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window



import re

def get_latest_ingest_prefix(bucket: str, base_prefix: str) -> str:
    """
    Return the latest ingest_dt=YYYY[/MM[/DD]] prefix under base_prefix by
    scanning object keys (no Delimiter) and extracting the ingest_dt portion.

    Example keys:
      landing/bls/pr/ingest_dt=2025/11/20/pr.data.1.AllData
      landing/population/ingest_dt=2025/11/20/population.json
    """
    s3 = boto3.client("s3")

    # List all objects under base_prefix + 'ingest_dt='
    prefix = base_prefix + "ingest_dt="
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    contents = resp.get("Contents", [])
    if not contents:
        raise RuntimeError(f"No objects found under s3://{bucket}/{prefix}")

    candidates = []

    for obj in contents:
        key = obj["Key"]  # e.g. landing/population/ingest_dt=2025/11/20/population.json

        m = re.search(r"ingest_dt=(\d{4})(?:/(\d{2}))?(?:/(\d{2}))?", key)
        if not m:
            continue

        year = int(m.group(1))
        month = int(m.group(2)) if m.group(2) else 1
        day = int(m.group(3)) if m.group(3) else 1

        dt = datetime(year, month, day)

        # Normalize to a full ingest_dt=YYYY/MM/DD/ prefix
        full_prefix = f"{base_prefix}ingest_dt={year:04d}/{month:02d}/{day:02d}/"
        candidates.append((dt, full_prefix))

    if not candidates:
        raise RuntimeError(f"No ingest_dt patterns found under s3://{bucket}/{prefix}")

    # Pick the latest date
    latest_dt, latest_prefix = max(candidates, key=lambda x: x[0])
    return latest_prefix




def read_bls_time_series(spark, bucket: str, base_prefix: str):
    """
    Read BLS 'pr.data.1.AllData' into a DataFrame:
      series_id, year (int), period, value (double)
    """
    latest_prefix = get_latest_ingest_prefix(bucket, base_prefix)
    path = f"s3://{bucket}/{latest_prefix}pr.data.1.AllData"

    # BLS data files are whitespace-separated, with a header line:
    # series_id  year  period  value  footnote_codes
    # We'll read as text and split on whitespace.
    df_text = spark.read.text(path)

    parts = F.split(F.trim(F.col("value")), r"\s+")
    df = (
        df_text
        .filter(F.col("value").rlike(r"^[A-Za-z0-9]"))  # skip blank lines
        .select(
            parts.getItem(0).alias("series_id"),
            parts.getItem(1).alias("year_str"),
            parts.getItem(2).alias("period"),
            parts.getItem(3).alias("value_str"),
        )
        .withColumn("year", F.col("year_str").cast("int"))
        .withColumn("value", F.col("value_str").cast("double"))
        .drop("year_str", "value_str")
    )

    return df


def read_population_us(spark, bucket: str, base_prefix: str):
    """
    Read population JSON from landing into a DataFrame:
      year (int), population (long)
    The API response structure is: {"data": [ { "Year": ..., "Population": ... }, ... ]}
    """
    latest_prefix = get_latest_ingest_prefix(bucket, base_prefix)
    path = f"s3://{bucket}/{latest_prefix}population.json"

    df_json = spark.read.json(path)

    # If the root has a 'data' array, explode it
    if "data" in df_json.columns:
        df = (
            df_json
            .select(F.explode("data").alias("d"))
            .select(
                F.col("d.Year").cast("int").alias("year"),
                F.col("d.Population").cast("long").alias("population"),
            )
        )
    else:
        # Flat records
        df = (
            df_json
            .select(
                F.col("Year").cast("int").alias("year"),
                F.col("Population").cast("long").alias("population"),
            )
        )

    return df


def build_report_best_year(time_series_df):
    """
    For each series_id:
      - sum value by year
      - pick the year with maximum summed value (ties → latest year)
    """
    agg = (
        time_series_df
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("year_sum"))
    )

    win = Window.partitionBy("series_id").orderBy(F.desc("year_sum"), F.desc("year"))

    best = (
        agg
        .withColumn("rn", F.row_number().over(win))
        .filter("rn = 1")
        .select(
            "series_id",
            F.col("year").alias("best_year"),
            F.col("year_sum").alias("summed_value_for_best_year"),
        )
    )

    return best


def build_report_joined(time_series_df, population_df):
    """
    Join time_series_pr with population_us on year.
    Columns:
      series_id, period, year, value, population
    """
    joined = (
        time_series_df.alias("t")
        .join(population_df.alias("p"), on="year", how="left")
        .select(
            "t.series_id",
            "t.period",
            "t.year",
            "t.value",
            "p.population",
        )
    )
    return joined


def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "ENV",
        "LANDING_BUCKET",
        "PROCESSED_BUCKET",
        "ANALYTICS_BUCKET",
    ])

    env = args["ENV"]
    landing_bucket = args["LANDING_BUCKET"]
    processed_bucket = args["PROCESSED_BUCKET"]
    analytics_bucket = args["ANALYTICS_BUCKET"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    print(f"Starting Data Quest Glue job for env={env}")
    print(f"Landing bucket:   {landing_bucket}")
    print(f"Processed bucket: {processed_bucket}")
    print(f"Analytics bucket: {analytics_bucket}")

    # 1) Read BLS and population from latest ingest_dt
    time_series_df = read_bls_time_series(spark, landing_bucket, "landing/bls/pr/")
    population_df = read_population_us(spark, landing_bucket, "landing/population/")

    # 2) Write processed (Silver) tables
    # time_series_pr
    ts_out = f"s3://{processed_bucket}/processed/bls/time_series_pr/"
    (
        time_series_df
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(ts_out)
    )

    # population_us
    pop_out = f"s3://{processed_bucket}/processed/population/population_us/"
    (
        population_df
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(pop_out)
    )

    # 3) Build Gold reports
    best_year_df = build_report_best_year(time_series_df)
    joined_df = build_report_joined(time_series_df, population_df)

    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    reports_base = f"s3://{analytics_bucket}/analytics/reports/run_date={run_date}/"

    best_out = reports_base + "report_best_year.csv"
    joined_out = reports_base + "report_joined.csv"

    (
        best_year_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(best_out)
    )

    (
        joined_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(joined_out)
    )

    print(f"Wrote time_series_pr to   {ts_out}")
    print(f"Wrote population_us to    {pop_out}")
    print(f"Wrote report_best_year to {best_out}")
    print(f"Wrote report_joined to    {joined_out}")

    job.commit()


if __name__ == "__main__":
    main()
git