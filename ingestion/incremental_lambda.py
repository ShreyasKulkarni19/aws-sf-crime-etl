import json
import boto3
import logging
import urllib.request
import urllib.parse
import os
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------

RAW_BUCKET = os.environ["RAW_BUCKET"]
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
API_BASE_URL = os.environ["API_BASE_URL"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE"))
PIPELINE_NAME = os.environ.get("PIPELINE_NAME")
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "10"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)

# ----------------------------
# DynamoDB Helpers
# ----------------------------

def get_checkpoint():
    response = table.get_item(
        Key={
            "pipeline_name": PIPELINE_NAME,
            "record_type": "incremental_checkpoint"
        }
    )
    item = response.get("Item")
    if item:
        return item.get("last_loaded_at"), item.get("last_loaded_id")
    return None, None


def update_checkpoint(timestamp, last_id):
    table.put_item(
        Item={
            "pipeline_name": PIPELINE_NAME,
            "record_type": "incremental_checkpoint",
            "last_loaded_at": timestamp,
            "last_loaded_id": last_id
        }
    )


def should_update_checkpoint(ts, record_id, max_ts, max_id):
    if not ts:
        return False
    if not max_ts:
        return True
    if ts > max_ts:
        return True
    if ts == max_ts and record_id and (not max_id or record_id > max_id):
        return True
    return False


def fetch_with_retries(url):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                if response.status != 200:
                    raise Exception(f"API returned status {response.status}")
                return json.loads(response.read().decode())
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                logger.warning(f"API request failed (attempt {attempt}). Retrying.")
            else:
                logger.error(f"API request failed after {MAX_RETRIES} attempts: {str(e)}")
    raise last_error

# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    logger.info("Incremental ingestion started")

    last_loaded_at, last_loaded_id = get_checkpoint()
    logger.info(f"Last checkpoint: {last_loaded_at}, id={last_loaded_id}")

    offset = 0
    max_loaded_at = last_loaded_at
    max_loaded_id = last_loaded_id

    while True:

        params = {
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": "data_loaded_at ASC, incident_id ASC"
        }

        from datetime import timedelta

        if last_loaded_at:
            if last_loaded_id:
                where_clause = (
                    f"(data_loaded_at > '{last_loaded_at}') OR "
                    f"(data_loaded_at = '{last_loaded_at}' AND incident_id > '{last_loaded_id}')"
                )
            else:
                where_clause = f"data_loaded_at > '{last_loaded_at}'"
        else:
            # First run fallback
            fallback_time = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
            where_clause = f"data_loaded_at > '{fallback_time}'"
            logger.info(f"No checkpoint found. Defaulting to last 1 day from {fallback_time}")

        params["$where"] = where_clause

        query_string = urllib.parse.urlencode(params)
        url = f"{API_BASE_URL}?{query_string}"

        data = fetch_with_retries(url)

        if not data:
            break

        logger.info(f"Fetched {len(data)} records")

        for record in data:
            ts = record.get("data_loaded_at")
            record_id = record.get("incident_id")
            if should_update_checkpoint(ts, record_id, max_loaded_at, max_loaded_id):
                max_loaded_at = ts
                max_loaded_id = record_id

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        s3_key = f"incremental/{timestamp}_{offset}.json"

        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        offset += BATCH_SIZE

        if len(data) < BATCH_SIZE:
            break

    if max_loaded_at:
        update_checkpoint(max_loaded_at, max_loaded_id)
        logger.info(f"Updated checkpoint to {max_loaded_at}, id={max_loaded_id}")
    else:
        logger.info("No new records found")

    return {"statusCode": 200}