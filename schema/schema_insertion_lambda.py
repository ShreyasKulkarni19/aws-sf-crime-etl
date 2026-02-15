import boto3
import logging
import os
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["SCHEMA_TABLE"]
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "sf_crime")

table = dynamodb.Table(TABLE_NAME)


# ----------------------------
# Define RAW Schema
# ----------------------------

RAW_SCHEMA = {
    "row_id": "STRING",
    "incident_datetime": "TIMESTAMP",
    "incident_date": "TIMESTAMP",
    "incident_time": "STRING",
    "incident_year": "STRING",
    "incident_day_of_week": "STRING",
    "report_datetime": "TIMESTAMP",
    "incident_id": "STRING",
    "incident_number": "STRING",
    "cad_number": "STRING",
    "report_type_code": "STRING",
    "report_type_description": "STRING",
    "filed_online": "BOOLEAN",
    "incident_code": "STRING",
    "incident_category": "STRING",
    "incident_subcategory": "STRING",
    "incident_description": "STRING",
    "resolution": "STRING",
    "intersection": "STRING",
    "cnn": "STRING",
    "police_district": "STRING",
    "analysis_neighborhood": "STRING",
    "supervisor_district": "STRING",
    "supervisor_district_2012": "STRING",
    "latitude": "FLOAT",
    "longitude": "FLOAT",
    "point": "GEOJSON",
    "data_as_of": "TIMESTAMP",
    "data_loaded_at": "TIMESTAMP"
}


# ----------------------------
# Define CURATED Schema
# ----------------------------

CURATED_SCHEMA = {
    "row_id": "STRING",
    "incident_date": "DATE",
    "incident_time": "TIME",
    "incident_year": "INTEGER",
    "incident_month": "INTEGER",
    "incident_day": "INTEGER",
    "incident_hour": "INTEGER",
    "incident_quarter": "INTEGER",
    "incident_day_of_week": "STRING",
    "report_date": "DATE",
    "report_time": "TIME",
    "incident_id": "STRING",
    "incident_number": "STRING",
    "cad_number": "STRING",
    "report_type_code": "STRING",
    "report_type_description": "STRING",
    "incident_code": "STRING",
    "incident_category": "STRING",
    "incident_subcategory": "STRING",
    "incident_description": "STRING",
    "resolution": "STRING",
    "intersection": "STRING",
    "cnn": "STRING",
    "police_district": "STRING",
    "analysis_neighborhood": "STRING",
    "supervisor_district": "INTEGER",
    "supervisor_district_2012": "INTEGER",
    "latitude": "FLOAT",
    "longitude": "FLOAT",
    "point": "GEOJSON",
    "data_as_of": "TIMESTAMP",
    "data_loaded_at": "TIMESTAMP",
    "is_weekend": "BOOLEAN",
    "is_violent_crime": "BOOLEAN",
    "is_peak_hour": "BOOLEAN"
}


# ----------------------------
# Insert Schema Helper
# ----------------------------

def insert_schema(schema_type, schema_dict, version=1):

    try:
        table.put_item(
            Item={
                "pipeline_name": PIPELINE_NAME,
                "schema_type": schema_type,
                "version": version,
                "created_at": datetime.utcnow().isoformat(),
                "schema": schema_dict
            },
            ConditionExpression="attribute_not_exists(schema_type)"
        )

        logger.info(f"Inserted schema: {schema_type}")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(f"Schema {schema_type} already exists. Skipping.")
            return False
        else:
            logger.error(f"Error inserting schema {schema_type}: {str(e)}")
            raise


# ----------------------------
# Lambda Handler
# ----------------------------

def lambda_handler(event, context):

    insert_schema("raw_v1", RAW_SCHEMA, 1)
    insert_schema("curated_v1", CURATED_SCHEMA, 1)

    return {
        "statusCode": 200,
        "message": "Schemas inserted successfully"
    }