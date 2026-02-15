import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client("dynamodb")

TABLE_NAME = os.environ.get("SCHEMA_TABLE", "sf-crime-schema-registry")

def lambda_handler(event, context):

    logger.info(f"Checking if table {TABLE_NAME} exists")

    try:
        dynamodb.describe_table(TableName=TABLE_NAME)
        logger.info("Table already exists")
        return {"statusCode": 200, "message": "Table already exists"}

    except dynamodb.exceptions.ResourceNotFoundException:
        logger.info("Table does not exist. Creating table...")

        response = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    "AttributeName": "pipeline_name",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "schema_type",
                    "KeyType": "RANGE"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "pipeline_name",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "schema_type",
                    "AttributeType": "S"
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        logger.info("Waiting for table to become ACTIVE")

        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=TABLE_NAME)

        logger.info("Table created successfully")

        return {
            "statusCode": 200,
            "message": "Table created"
        }

    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise