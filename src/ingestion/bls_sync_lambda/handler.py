import os
import logging
import json
from datetime import datetime
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"

secrets_client = boto3.client("secretsmanager")
s3 = boto3.client("s3")


def http_get(url: str, headers: dict | None = None, timeout: int = 30) -> tuple[int, str]:
    """Simple HTTP GET using stdlib urllib (no requests dependency)."""
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        status = resp.status  # type: ignore[attr-defined]
        text = resp.read().decode("utf-8", errors="replace")
        return status, text


def http_get_bytes(url: str, headers: dict | None = None, timeout: int = 60) -> tuple[int, bytes]:
    """GET returning raw bytes (for file download)."""
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        status = resp.status  # type: ignore[attr-defined]
        data = resp.read()
        return status, data


def get_bls_user_agent(secret_arn: str) -> str:
    """
    Read User-Agent config from Secrets Manager.
    Secret JSON looks like:
      { "blsUserAgent": "DataQuest-dev (Email: you@example.com; Phone: ...)" }
    """
    resp = secrets_client.get_secret_value(SecretId=secret_arn)
    data = json.loads(resp["SecretString"])
    return data.get("blsUserAgent", "DataQuest-DataQuestDemo/1.0")



import re

def list_bls_files(headers: dict) -> list[str]:
    """
    Fetch the directory listing and return BLS time-series filenames.

    The HTML is a directory listing page that contains file references like:
      pr.class
      pr.data.0.Current
      pr.data.1.AllData
      pr.series
    etc.

    We avoid trying to parse HTML structure and instead scan the entire
    response text with a regex for tokens that look like 'pr.<something>'.
    """
    status, text = http_get(BLS_BASE_URL, headers=headers, timeout=30)
    if status == 403:
        logger.error("Got 403 from BLS. Check User-Agent per BLS policy.")
        raise Exception("403 Forbidden from BLS")
    if status != 200:
        raise Exception(f"BLS index returned HTTP {status}")

    # Find all substrings that look like pr.<letters/digits/underscore/dot>
    matches = re.findall(r"pr\.[A-Za-z0-9_.]+", text)

    # Deduplicate & sort for stability
    files = sorted(set(matches))

    logger.info(f"Found {len(files)} BLS files under {BLS_BASE_URL}: {files}")
    return files





def download_file(filename: str, headers: dict) -> bytes:
    url = urljoin(BLS_BASE_URL, filename)
    logger.info(f"Downloading {url}")
    status, data = http_get_bytes(url, headers=headers, timeout=60)
    if status != 200:
        raise Exception(f"Download of {filename} failed with HTTP {status}")
    return data


def lambda_handler(event, context):
    env = os.getenv("ENV", "dev")
    landing_bucket = os.getenv("LANDING_BUCKET")
    config_secret_arn = os.getenv("CONFIG_SECRET_ARN")

    if not landing_bucket:
        raise RuntimeError("LANDING_BUCKET env var not set")
    if not config_secret_arn:
        raise RuntimeError("CONFIG_SECRET_ARN env var not set")

    logger.info(f"Starting BLS sync. ENV={env}, landing_bucket={landing_bucket}")

    user_agent = get_bls_user_agent(config_secret_arn)
    headers = {"User-Agent": user_agent}

    # 1) List files
    files = list_bls_files(headers)

    # 2) Download & upload to landing/bls/pr/ingest_dt=YYYY/MM/DD/
    today = datetime.utcnow()
    ingest_prefix = f"landing/bls/pr/ingest_dt={today:%Y/%m/%d}/"
    uploaded: list[str] = []

    for fname in files:
        content = download_file(fname, headers=headers)
        key = ingest_prefix + fname

        logger.info(f"Uploading {fname} to s3://{landing_bucket}/{key}")
        s3.put_object(
            Bucket=landing_bucket,
            Key=key,
            Body=content,
        )
        uploaded.append(key)

    return {
        "status": "ok",
        "env": env,
        "landing_bucket": landing_bucket,
        "uploaded_keys": uploaded,
    }
