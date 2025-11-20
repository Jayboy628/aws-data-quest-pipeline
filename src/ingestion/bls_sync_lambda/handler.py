import os
import logging
import boto3
import requests
from datetime import datetime
from urllib.parse import urljoin

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"


secrets_client = boto3.client("secretsmanager")
s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")


def get_bls_user_agent(secret_arn: str) -> str:
    """
    Read User-Agent config from Secrets Manager.
    Secret JSON looks like:
      { "blsUserAgent": "DataQuest-dev (Email: you@example.com; Phone: ...)" }
    """
    resp = secrets_client.get_secret_value(SecretId=secret_arn)
    secret_str = resp["SecretString"]
    import json
    data = json.loads(secret_str)
    return data.get("blsUserAgent", "DataQuest-Unknown/1.0")


def list_bls_files(headers: dict) -> list[str]:
    """
    Fetch the directory listing and return file names.
    For the BLS time.series/pr/ directory, the content is a plain HTML index.
    We'll do a simple parse: find links that look like 'pr.*'.
    """
    resp = requests.get(BLS_BASE_URL, headers=headers, timeout=30)
    if resp.status_code == 403:
        logger.error("Got 403 from BLS. Check User-Agent per BLS policy.")
        raise Exception("403 Forbidden from BLS")
    resp.raise_for_status()

    text = resp.text
    files = []
    for line in text.splitlines():
        # crude but works for the simple directory index:
        # look for href="pr.xxx"
        if 'href="pr.' in line:
            # e.g. <a href="pr.data.0.Current">pr.data.0.Current</a>
            start = line.find('href="') + len('href="')
            end = line.find('"', start)
            name = line[start:end]
            if name.startswith("pr."):
                files.append(name)
    logger.info(f"Found {len(files)} candidate files: {files}")
    return files


def download_file(filename: str, headers: dict) -> bytes:
    url = urljoin(BLS_BASE_URL, filename)
    logger.info(f"Downloading {url}")
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.content


def lambda_handler(event, context):
    env = os.getenv("ENV", "dev")
    landing_bucket = os.getenv("LANDING_BUCKET")
    manifest_table = os.getenv("MANIFEST_TABLE")   # not used yet in phase 1
    config_secret_arn = os.getenv("CONFIG_SECRET_ARN")

    if not landing_bucket:
        raise RuntimeError("LANDING_BUCKET env var not set")

    logger.info(f"Starting BLS sync. ENV={env}, landing_bucket={landing_bucket}")

    user_agent = get_bls_user_agent(config_secret_arn)
    headers = {"User-Agent": user_agent}

    # 1) List files in BLS directory
    files = list_bls_files(headers)

    # 2) For now: download ALL files each run and write to ingest_dt path.
    #    (You will later optimize using DynamoDB manifest to skip unchanged files.)
    today = datetime.utcnow()
    ingest_prefix = f"landing/bls/pr/ingest_dt={today:%Y/%m/%d}/"

    uploaded = []
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
