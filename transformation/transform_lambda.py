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

CURATED_BUCKET = os.environ["CURATED_BUCKET"]
PIPELINE_NAME = os.environ.get("PIPELINE_NAME")
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]

VIOLENT_CRIMES = {"HOMICIDE", "ASSAULT", "ROBBERY", "KIDNAPPING", "SEX_OFFENSES"}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)

# ----------------------------
# Idempotency
# ----------------------------

def already_processed(raw_key):
    resp = table.get_item(
        Key={
            "pipeline_name": PIPELINE_NAME,
            "record_type": f"processed_file#{raw_key}"
        }
    )
    return "Item" in resp


def write_marker(raw_key):
    table.put_item(
        Item={
            "pipeline_name": PIPELINE_NAME,
            "record_type": f"processed_file#{raw_key}",
            "processed_at": datetime.utcnow().isoformat()
        },
        ConditionExpression="attribute_not_exists(record_type)"
    )

# ----------------------------
# Helpers
# ----------------------------

def normalize_text(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def parse_datetime(dt_string):
    if not dt_string:
        return None
    try:
        return datetime.fromisoformat(dt_string.replace("Z", ""))
    except Exception:
        return None


# ----------------------------
# Transformation Logic
# ----------------------------

def transform_record(record):

    incident_dt = parse_datetime(record.get("incident_datetime"))
    if not incident_dt:
        return None

    report_dt = parse_datetime(record.get("report_datetime"))

    try:
        lat = float(record.get("latitude"))
        lon = float(record.get("longitude"))
    except (TypeError, ValueError):
        return None

    category = normalize_text(record.get("incident_category"))
    category_norm = (category or "UNKNOWN").upper()

    transformed = {}

    # ---- Identifiers ----
    transformed["row_id"] = normalize_text(record.get("row_id"))
    transformed["incident_id"] = normalize_text(record.get("incident_id"))
    transformed["incident_number"] = normalize_text(record.get("incident_number"))
    transformed["cad_number"] = normalize_text(record.get("cad_number"))

    # ---- Codes & Text ----
    transformed["report_type_code"] = normalize_text(record.get("report_type_code"))
    transformed["report_type_description"] = normalize_text(record.get("report_type_description"))
    transformed["incident_code"] = normalize_text(record.get("incident_code"))
    transformed["incident_category"] = category_norm
    transformed["incident_subcategory"] = normalize_text(record.get("incident_subcategory"))
    transformed["incident_description"] = normalize_text(record.get("incident_description"))
    transformed["resolution"] = normalize_text(record.get("resolution"))

    transformed["intersection"] = normalize_text(record.get("intersection"))
    transformed["cnn"] = normalize_text(record.get("cnn"))
    transformed["police_district"] = normalize_text(record.get("police_district"))
    transformed["analysis_neighborhood"] = normalize_text(record.get("analysis_neighborhood"))

    # ---- Supervisor fields as INT ----
    try:
        transformed["supervisor_district"] = int(record.get("supervisor_district")) if record.get("supervisor_district") else None
    except:
        transformed["supervisor_district"] = None

    try:
        transformed["supervisor_district_2012"] = int(record.get("supervisor_district_2012")) if record.get("supervisor_district_2012") else None
    except:
        transformed["supervisor_district_2012"] = None

    # ---- Location ----
    transformed["latitude"] = lat
    transformed["longitude"] = lon
    transformed["point"] = record.get("point")

    # ---- Source Timestamps ----
    transformed["data_as_of"] = normalize_text(record.get("data_as_of"))
    transformed["data_loaded_at"] = normalize_text(record.get("data_loaded_at"))

    transformed["filed_online"] = record.get("filed_online")

    # ---- Derived Time Features ----
    transformed["incident_date"] = incident_dt.date().isoformat()
    transformed["incident_time"] = incident_dt.strftime("%H:%M:%S")
    transformed["incident_year"] = int(incident_dt.year)
    transformed["incident_month"] = int(incident_dt.month)
    transformed["incident_day"] = int(incident_dt.day)
    transformed["incident_hour"] = int(incident_dt.hour)
    transformed["incident_quarter"] = int((incident_dt.month - 1) // 3 + 1)
    transformed["incident_day_of_week"] = incident_dt.strftime("%A")
    transformed["is_weekend"] = incident_dt.weekday() >= 5

    if report_dt:
        transformed["report_date"] = report_dt.date().isoformat()
        transformed["report_time"] = report_dt.strftime("%H:%M:%S")
        transformed["report_delay_hours"] = (
            (report_dt - incident_dt).total_seconds() / 3600.0
        )
    else:
        transformed["report_date"] = None
        transformed["report_time"] = None
        transformed["report_delay_hours"] = None

    # ---- Business Flags ----
    transformed["is_violent_crime"] = category_norm in VIOLENT_CRIMES
    transformed["is_peak_hour"] = 17 <= incident_dt.hour <= 21

    return transformed


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    logger.info("Transform Lambda triggered")

    if "Records" not in event:
        return {"statusCode": 400}

    for s3_record in event["Records"]:

        bucket = s3_record["s3"]["bucket"]["name"]
        key = s3_record["s3"]["object"]["key"]

        if already_processed(key):
            continue

        resp = s3.get_object(Bucket=bucket, Key=key)
        raw_data = json.loads(resp["Body"].read())

        grouped = defaultdict(list)
        seen_ids = set()

        for record in raw_data:

            transformed = transform_record(record)
            if not transformed:
                continue

            incident_id = transformed.get("incident_id")
            if not incident_id or incident_id in seen_ids:
                continue

            seen_ids.add(incident_id)

            y = transformed["incident_year"]
            m = transformed["incident_month"]
            d = transformed["incident_day"]
            c = transformed["incident_category"]

            grouped[(y, m, d, c)].append(transformed)

        for (y, m, d, c), records_list in grouped.items():

            partition_path = (
                f"year={y}/"
                f"month={str(m).zfill(2)}/"
                f"day={str(d).zfill(2)}/"
            )

            safe_category = (c or "UNKNOWN").replace(" ", "_")

            filename = (
                f"{safe_category}-{y}-"
                f"{str(m).zfill(2)}-"
                f"{str(d).zfill(2)}-"
                f"{uuid.uuid4().hex}.json"
            )

            curated_key = partition_path + filename

            body = "\n".join(json.dumps(r) for r in records_list)

            s3.put_object(
                Bucket=CURATED_BUCKET,
                Key=curated_key,
                Body=body,
                ContentType="application/json"
            )

        write_marker(key)

    return {"statusCode": 200}