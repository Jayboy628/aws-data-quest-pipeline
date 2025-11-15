import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Stub for Population API Lambda.

    For now it just logs and returns a simple JSON response so that
    CloudFormation can create and invoke the function successfully.

    Later we will:
      - Call the population API
      - Write the JSON to S3 landing/population/ingest_dt=...
    """
    env = os.getenv("ENV", "dev")
    landing_bucket = os.getenv("LANDING_BUCKET", "unknown")

    logger.info("Population API lambda stub invoked")
    logger.info(f"ENV={env}, LANDING_BUCKET={landing_bucket}")

    return {
        "status": "ok",
        "message": "Population API lambda stub executed",
        "env": env,
        "landing_bucket": landing_bucket,
    }
