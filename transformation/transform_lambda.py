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
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])
PIPELINE_NAME = os.environ.get("PIPELINE_NAME")

# ----------------------------
# Idempotency Helpers
# ----------------------------

def already_processed(raw_key):
    response = table.get_item(
        Key={
            "pipeline_name": PIPELINE_NAME,
            "record_type": f"processed_file#{raw_key}"
        }
    )
    return "Item" in response


def write_marker(raw_key):
    try:
        table.put_item(
            Item={
                "pipeline_name": PIPELINE_NAME,
                "record_type": f"processed_file#{raw_key}",
                "processed_at": datetime.utcnow().isoformat()
            },
            ConditionExpression="attribute_not_exists(record_type)"
        )
        logger.info(f"DynamoDB marker written for {raw_key}")
    except Exception as e:
        logger.warning(f"File already processed or conditional write failed: {str(e)}")
        raise

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
    
    report_dt = parse_datetime(record.get("report_datetime"))

    # Convert lat/lon to float
    try:
        lat = float(record.get("latitude"))
        lon = float(record.get("longitude"))
    except (TypeError, ValueError):
        return None

    # ---- Start with full original record ----
    transformed = record.copy()

    transformed.pop("incident_datetime", None)
    transformed.pop("report_datetime", None)

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
    transformed["incident_date"] = dt.date().isoformat()
    transformed["incident_time"] = dt.strftime("%H:%M:%S")
    transformed["incident_year"] = int(dt.year)
    transformed["incident_month"] = int(dt.month)
    transformed["incident_day"] = int(dt.day)
    transformed["incident_hour"] = int(dt.hour)
    transformed["incident_quarter"] = int((dt.month - 1) // 3 + 1)
    transformed["incident_day_of_week"] = dt.strftime("%A")
    transformed["is_weekend"] = dt.weekday() >= 5
    if report_dt:
        transformed["report_date"] = report_dt.date().isoformat()
        transformed["report_time"] = report_dt.strftime("%H:%M:%S")

    try:
        transformed["supervisor_district"] = int(record.get("supervisor_district")) if record.get("supervisor_district") else None
    except:
        transformed["supervisor_district"] = None

    try:
        transformed["supervisor_district_2012"] = int(record.get("supervisor_district_2012")) if record.get("supervisor_district_2012") else None
    except:
        transformed["supervisor_district_2012"] = None

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
                category = "UNKNOWN"  # or "OTHER" or whatever default you want
                logger.info(f"Using default category for record with missing category")

            safe_category = category.replace(" ", "_")

            filename = f"{safe_category}-{year}-{str(month).zfill(2)}-{str(day).zfill(2)}-{uuid.uuid4().hex}.json"

            curated_key = partition_path + filename
            
            body = "\n".join(json.dumps(r) for r in records_list)

            s3.put_object(
                Bucket=CURATED_BUCKET,
                Key=curated_key,
                Body=body,
                ContentType="application/json"
            )

            logger.info(f"Wrote curated file: {curated_key}")

        # ---- Write Marker After Successful Processing ----
        write_marker(key)

    return {
        "statusCode": 200
    }