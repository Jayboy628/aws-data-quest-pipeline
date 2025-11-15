import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Stub for BLS sync Lambda.

    For now it just logs and returns a simple JSON response so that
    CloudFormation can create and invoke the function successfully.

    Later we will:
      - Read LANDING_BUCKET, MANIFEST_TABLE, CONFIG_SECRET_ARN from env
      - Call BLS, download files, update DynamoDB manifest, write to S3
    """
    env = os.getenv("ENV", "dev")
    landing_bucket = os.getenv("LANDING_BUCKET", "unknown")
    manifest_table = os.getenv("MANIFEST_TABLE", "unknown")

    logger.info("BLS sync lambda stub invoked")
    logger.info(f"ENV={env}, LANDING_BUCKET={landing_bucket}, MANIFEST_TABLE={manifest_table}")

    return {
        "status": "ok",
        "message": "BLS sync lambda stub executed",
        "env": env,
        "landing_bucket": landing_bucket,
        "manifest_table": manifest_table,
    }
