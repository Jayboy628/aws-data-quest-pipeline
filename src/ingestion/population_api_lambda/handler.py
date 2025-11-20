import os
import logging
import json
from datetime import datetime

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

POP_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)


def lambda_handler(event, context):
    env = os.getenv("ENV", "dev")
    landing_bucket = os.getenv("LANDING_BUCKET")

    if not landing_bucket:
        raise RuntimeError("LANDING_BUCKET env var not set")

    logger.info(f"Calling population API. ENV={env}, LANDING_BUCKET={landing_bucket}")

    resp = requests.get(POP_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Optional: validate structure has 'data' key or similar
    # For now, just store the full JSON response.
    today = datetime.utcnow()
    ingest_prefix = f"landing/population/ingest_dt={today:%Y/%m/%d}/"
    key = ingest_prefix + "population.json"

    logger.info(f"Writing population JSON to s3://{landing_bucket}/{key}")
    s3.put_object(
        Bucket=landing_bucket,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "status": "ok",
        "env": env,
        "landing_bucket": landing_bucket,
        "s3_key": key,
    }
