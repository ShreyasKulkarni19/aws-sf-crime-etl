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
        return item.get("last_loaded_at")
    return None


def update_checkpoint(timestamp):
    table.put_item(
        Item={
            "pipeline_name": PIPELINE_NAME,
            "record_type": "incremental_checkpoint",
            "last_loaded_at": timestamp
        }
    )

# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    logger.info("Incremental ingestion started")

    last_loaded_at = get_checkpoint()
    logger.info(f"Last checkpoint: {last_loaded_at}")

    offset = 0
    max_loaded_at = last_loaded_at
    all_records = []

    while True:

        params = {
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": "data_loaded_at ASC"
        }

        if last_loaded_at:
            params["$where"] = f"data_loaded_at > '{last_loaded_at}'"

        query_string = urllib.parse.urlencode(params)
        url = f"{API_BASE_URL}?{query_string}"

        try:
            with urllib.request.urlopen(url) as response:
                if response.status != 200:
                    raise Exception(f"API returned status {response.status}")
                data = json.loads(response.read().decode())
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise

        if not data:
            break

        logger.info(f"Fetched {len(data)} records")

        for record in data:
            ts = record.get("data_loaded_at")
            if ts and (not max_loaded_at or ts > max_loaded_at):
                max_loaded_at = ts

        all_records.extend(data)
        offset += BATCH_SIZE

        if len(data) < BATCH_SIZE:
            break

    if not all_records:
        logger.info("No new records found")
        return {"statusCode": 200}

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    s3_key = f"incremental/{timestamp}.json"

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=json.dumps(all_records),
        ContentType="application/json"
    )

    logger.info(f"Wrote incremental batch to {s3_key}")

    if max_loaded_at:
        update_checkpoint(max_loaded_at)
        logger.info(f"Updated checkpoint to {max_loaded_at}")

    return {"statusCode": 200}