import json
from unicodedata import category
import boto3
import logging
import os
from datetime import datetime
from collections import defaultdict
import uuid

# ----------------------------
# Configuration
# ----------------------------

RAW_BUCKET = os.environ["RAW_BUCKET"]
CURATED_BUCKET = os.environ["CURATED_BUCKET"]

VIOLENT_CRIMES = {
    "HOMICIDE",
    "ASSAULT",
    "ROBBERY",
    "KIDNAPPING",
    "SEX_OFFENSES"
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# ----------------------------
# Idempotency Helpers
# ----------------------------

def already_processed(raw_key):
    marker_key = f"processed/{raw_key.split('/')[-1]}.done"
    try:
        s3.head_object(Bucket=CURATED_BUCKET, Key=marker_key)
        logger.info(f"Marker exists: {marker_key}")
        return True
    except s3.exceptions.ClientError:
        return False


def write_marker(raw_key):
    marker_key = f"processed/{raw_key.split('/')[-1]}.done"
    s3.put_object(
        Bucket=CURATED_BUCKET,
        Key=marker_key,
        Body=b""
    )
    logger.info(f"Marker written: {marker_key}")

# ----------------------------
# Helper Functions
# ----------------------------

def normalize_text(value):
    if not value:
        return None
    return str(value).strip()


def parse_datetime(dt_string):
    if not dt_string:
        return None
    try:
        return datetime.fromisoformat(dt_string.replace("Z", ""))
    except Exception:
        return None


def transform_record(record):
    dt = parse_datetime(record.get("incident_datetime"))
    if not dt:
        return None

    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is None or lon is None:
        return None

    # ---- Start with full original record ----
    transformed = record.copy()

    # ---- Normalize categorical columns ----
    text_fields = [
        "incident_category",
        "incident_subcategory",
        "incident_description",
        "resolution",
        "police_district",
        "analysis_neighborhood",
        "supervisor_district",
        "supervisor_district_2012",
        "report_type_code",
        "report_type_description"
    ]

    for field in text_fields:
        transformed[field] = normalize_text(record.get(field))

    # ---- Add derived datetime features ----
    transformed["incident_year"] = dt.year
    transformed["incident_month"] = dt.month
    transformed["incident_day"] = dt.day
    transformed["incident_hour"] = dt.hour
    transformed["incident_quarter"] = (dt.month - 1) // 3 + 1
    transformed["incident_day_of_week"] = dt.strftime("%A").upper()
    transformed["is_weekend"] = dt.weekday() >= 5

    # ---- Business enrichment ----
    category = transformed.get("incident_category") or "UNKNOWN"

    transformed["is_violent_crime"] = category in VIOLENT_CRIMES
    transformed["is_peak_hour"] = 17 <= dt.hour <= 21

    return transformed


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    logger.info("Transform Lambda triggered")

    if "Records" not in event:
        logger.warning("No S3 records found in event")
        return {"statusCode": 400}

    for s3_record in event["Records"]:

        bucket = s3_record["s3"]["bucket"]["name"]
        key = s3_record["s3"]["object"]["key"]

        logger.info(f"Processing raw file: {key}")

        # ---- Idempotency Check ----
        if already_processed(key):
            logger.info("File already processed. Skipping.")
            continue

        response = s3.get_object(Bucket=bucket, Key=key)
        raw_data = json.loads(response["Body"].read())

        grouped_records = defaultdict(list)
        seen_ids = set()

        for record in raw_data:

            transformed = transform_record(record)
            if not transformed:
                continue

            incident_id = transformed.get("incident_id")
            if not incident_id or incident_id in seen_ids:
                continue

            seen_ids.add(incident_id)

            year = transformed["incident_year"]
            month = transformed["incident_month"]
            day = transformed["incident_day"]
            category = transformed["incident_category"]

            group_key = (year, month, day, category)
            grouped_records[group_key].append(transformed)

        # ---- Write Curated Files ----
        for (year, month, day, category), records_list in grouped_records.items():

            partition_path = (
                f"year={year}/"
                f"month={str(month).zfill(2)}/"
                f"day={str(day).zfill(2)}/"
            )

            if category is None or category == "":
                logger.warning(f"Skipping record with missing category: {record}")
                continue

            safe_category = category.replace(" ", "_")
            filename = f"{safe_category}-{year}-{str(month).zfill(2)}-{str(day).zfill(2)}-{uuid.uuid4().hex}.json"

            curated_key = partition_path + filename

            s3.put_object(
                Bucket=CURATED_BUCKET,
                Key=curated_key,
                Body=json.dumps(records_list),
                ContentType="application/json"
            )

            logger.info(f"Wrote curated file: {curated_key}")

        # ---- Write Marker After Successful Processing ----
        write_marker(key)

    return {
        "statusCode": 200
    }