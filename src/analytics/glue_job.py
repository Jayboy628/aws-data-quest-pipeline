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


def get_latest_ingest_prefix(bucket: str, base_prefix: str) -> str:
    """
    Return the latest ingest_dt=YYYY/MM/DD prefix under base_prefix.
    Example base_prefix: 'landing/bls/pr/'
    """
    s3 = boto3.client("s3")

    # List all 'ingest_dt=' prefixes
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=base_prefix,
        Delimiter="/"
    )

    prefixes = []
    for cp in resp.get("CommonPrefixes", []):
        p = cp["Prefix"]  # e.g. landing/bls/pr/ingest_dt=2025/11/20/
        if "ingest_dt=" in p:
            prefixes.append(p)

    if not prefixes:
        raise RuntimeError(f"No ingest_dt prefixes found under s3://{bucket}/{base_prefix}")

    # Extract the date part after 'ingest_dt=' and normalize to a sortable key
    def parse_ingest(p):
        # p like 'landing/bls/pr/ingest_dt=2025/11/20/'
        part = p.split("ingest_dt=")[1].strip("/")
        # '2025/11/20'
        return datetime.strptime(part, "%Y/%m/%d")

    latest_prefix = max(prefixes, key=parse_ingest)
    return latest_prefix  # e.g. 'landing/bls/pr/ingest_dt=2025/11/20/'


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