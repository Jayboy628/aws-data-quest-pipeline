import os
import logging
import json
from datetime import datetime
from urllib.request import Request, urlopen

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

POP_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)

MANIFEST_TABLE = os.getenv("MANIFEST_TABLE", "dq_manifest_dev")


def http_get(url: str, timeout: int = 30) -> tuple[int, str]:
    """Simple HTTP GET using urllib."""
    req = Request(url)
    with urlopen(req, timeout=timeout) as resp:
        status = resp.status  # type: ignore[attr-defined]
        text = resp.read().decode("utf-8", errors="replace")
        return status, text


def write_manifest(source_path: str, status: str, notes: str, env: str) -> None:
    """
    Write a single manifest record to DynamoDB.
    Primary key is 'source_path' (HASH) as defined in dq_manifest_dev.
    """
    if not MANIFEST_TABLE:
        logger.warning("MANIFEST_TABLE env var not set; skipping manifest write")
        return

    item = {
        "source_path": {"S": source_path},
        "status": {"S": status},
        "notes": {"S": notes},
        "env": {"S": env},
        "updated_at": {"S": datetime.utcnow().isoformat()},
    }

    try:
        logger.info(f"Writing manifest entry to {MANIFEST_TABLE}: {item}")
        dynamodb.put_item(TableName=MANIFEST_TABLE, Item=item)
    except Exception as e:
        # Do not fail the whole Lambda if manifest logging fails
        logger.exception(f"Failed to write manifest for {source_path}: {e}")


def lambda_handler(event, context):
    env = os.getenv("ENV", "dev")
    landing_bucket = os.getenv("LANDING_BUCKET")

    if not landing_bucket:
        raise RuntimeError("LANDING_BUCKET env var not set")

    logger.info(f"Calling population API. ENV={env}, LANDING_BUCKET={landing_bucket}")

    status, text = http_get(POP_URL, timeout=30)
    if status != 200:
        raise Exception(f"Population API returned HTTP {status}")

    data = json.loads(text)

    today = datetime.utcnow()
    ingest_prefix = f"landing/population/ingest_dt={today:%Y/%m/%d}/"
    key = ingest_prefix + "population.json"
    s3_path = f"s3://{landing_bucket}/{key}"

    logger.info(f"Writing population JSON to {s3_path}")
    s3.put_object(
        Bucket=landing_bucket,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )

    # 🔹 NEW: write manifest row
    write_manifest(
        source_path=s3_path,
        status="SUCCESS",
        notes="population_api_lambda",
        env=env,
    )

    return {
        "status": "ok",
        "env": env,
        "landing_bucket": landing_bucket,
        "s3_key": key,
    }
