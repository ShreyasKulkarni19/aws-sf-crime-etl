import json
import boto3
import urllib.request
import logging
import os
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------

RAW_BUCKET = os.environ.get("RAW_BUCKET")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
BASE_URL = os.environ.get("API_BASE_URL")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE"))
PIPELINE_NAME = os.environ.get("PIPELINE_NAME")

# ----------------------------
# Logging Setup
# ----------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ----------------------------
# AWS Clients
# ----------------------------

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)

# ----------------------------
# DynamoDB Helper
# ----------------------------

def update_checkpoint(timestamp):
    logger.info(f"Updating DynamoDB checkpoint to {timestamp}")

    table.put_item(
        Item={
            "pipeline_name": PIPELINE_NAME,
            "record_type": "incremental_checkpoint",
            "last_loaded_at": timestamp
        }
    )

    logger.info("Checkpoint updated successfully")


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    logger.info("Backfill ingestion started")

    offset = 0
    batch_number = 1
    ingestion_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

    max_loaded_at = None  # <-- Track high watermark

    while True:

        url = f"{BASE_URL}?$limit={BATCH_SIZE}&$offset={offset}"
        logger.info(f"Fetching batch {batch_number}, offset={offset}")

        try:
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise

        if not data:
            logger.info("No more data returned from API")
            break

        # ----------------------------
        # Track max(data_loaded_at)
        # ----------------------------
        for record in data:
            ts = record.get("data_loaded_at")
            if ts and (not max_loaded_at or ts > max_loaded_at):
                max_loaded_at = ts

        s3_key = f"backfill/{ingestion_timestamp}_batch_{batch_number}.json"

        try:
            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType="application/json"
            )
        except Exception as e:
            logger.error(f"Failed to write batch {batch_number} to S3: {str(e)}")
            raise

        logger.info(f"Batch {batch_number} written with {len(data)} records")

        offset += BATCH_SIZE
        batch_number += 1

        if len(data) < BATCH_SIZE:
            logger.info("Last batch detected (less than batch size)")
            break

    # ----------------------------
    # Update checkpoint AFTER all batches succeed
    # ----------------------------
    if max_loaded_at:
        update_checkpoint(max_loaded_at)
        logger.info(f"Backfill completed. Final watermark: {max_loaded_at}")
    else:
        logger.warning("No data found. Checkpoint not updated.")

    logger.info("Backfill ingestion completed")

    return {
        "statusCode": 200,
        "batches_written": batch_number - 1,
        "final_checkpoint": max_loaded_at
    }