import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


def main():
    """
    Stub Glue job for Data Quest.

    For now it just prints some messages.
    Later we will:
      - Read landing BLS + population data
      - Build time_series_pr, population_us
      - Write processed and analytics outputs
    """
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    print("Data Quest Glue analytics job stub is running.")
    print("TODO: implement transformations and writes to processed + analytics buckets.")

    job.commit()


if __name__ == "__main__":
    main()
