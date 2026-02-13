import json
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
# Helper Functions
# ----------------------------

def normalize_text(value):
    if not value:
        return None
    return str(value).strip().upper().replace(" ", "_")


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


    # Require lat/lon
    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is None or lon is None:
        return None

    category = normalize_text(record.get("incident_category")) or "UNKNOWN"

    transformed = {
        "incident_id": record.get("incident_id"),
        "incident_datetime": record.get("incident_datetime"),
        "incident_year": dt.year,
        "incident_month": dt.month,
        "incident_day": dt.day,
        "incident_hour": dt.hour,
        "incident_quarter": (dt.month - 1) // 3 + 1,
        "incident_day_of_week": dt.strftime("%A").upper(),
        "is_weekend": dt.weekday() >= 5,
        "incident_category": category,
        "incident_subcategory": normalize_text(record.get("incident_subcategory")),
        "resolution": normalize_text(record.get("resolution")),
        "police_district": normalize_text(record.get("police_district")),
        "analysis_neighborhood": normalize_text(record.get("analysis_neighborhood")),
        "latitude": lat,
        "longitude": lon,
        "is_violent_crime": category in VIOLENT_CRIMES,
        "is_peak_hour": 17 <= dt.hour <= 21
    }

    return transformed


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    logger.info("Transform Lambda triggered")

    for s3_record in event["Records"]:

        bucket = s3_record["s3"]["bucket"]["name"]
        key = s3_record["s3"]["object"]["key"]

        logger.info(f"Processing raw file: {key}")

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

        # Write grouped files
        for (year, month, day, category), records_list in grouped_records.items():

            partition_path = (
                f"year={year}/"
                f"month={str(month).zfill(2)}/"
                f"day={str(day).zfill(2)}/"
            )

            filename = (
                f"{category}-"
                f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}-"
                f"{uuid.uuid4().hex}.json"
            )

            curated_key = partition_path + filename

            s3.put_object(
                Bucket=CURATED_BUCKET,
                Key=curated_key,
                Body=json.dumps(records_list),
                ContentType="application/json"
            )

            logger.info(f"Wrote curated file: {curated_key}")

    return {
        "statusCode": 200
    }