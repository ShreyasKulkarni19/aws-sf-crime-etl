import json
import boto3
import urllib.request
import logging
import os
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------

RAW_BUCKET = os.environ.get("RAW_BUCKET", "sf-crime-raw")
BASE_URL = "https://data.sfgov.org/resource/wg3w-h783.json"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 5000))

# ----------------------------
# Logging Setup
# ----------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ----------------------------
# AWS Client
# ----------------------------

s3 = boto3.client("s3")


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):
    """
    Backfill Lambda:
    - Pulls entire dataset from SF Open Data API
    - Uses offset-based pagination
    - Writes batch files to S3 raw bucket
    """

    logger.info("Backfill ingestion started")

    offset = 0
    batch_number = 1
    ingestion_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

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

    logger.info("Backfill ingestion completed")

    return {
        "statusCode": 200,
        "batches_written": batch_number - 1
    }