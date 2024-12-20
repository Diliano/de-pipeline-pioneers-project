import src.ingestion.utils as util
from datetime import (
    datetime,
)
import boto3
import json
import logging
import os

SECRET_NAME = os.getenv("DB_SECRET_NAME", "nc-totesys-db-credentials")
REGION_NAME = os.getenv("AWS_REGION", "eu-west-2")

TIMESTAMP_FILE_KEY = "metadata/last_ingestion_timestamp.json"

S3_INGESTION_BUCKET = os.getenv(
    "S3_INGESTION_BUCKET"
)  # MAKE SURE THIS IS DEFINED IN THE LAMBDA CODE FOR TF

# For testing
# S3_INGESTION_BUCKET = (
#     "nc-pipeline-pioneers-ingestion20241112120531000200000003"
# )

TABLES = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3_client = boto3.client("s3", region_name=REGION_NAME)
secrets_manager_client = boto3.client(
    "secretsmanager", region_name=REGION_NAME
)


def lambda_handler(event, context):
    """
    AWS Lambda function handler for ingesting database table data into S3.
    This function:
    - Fetches data from specified tables updated since the last ingestion.
    - Writes the data to an S3 bucket in JSON format.
    - Organizes the S3 keys by date and table name.
    - Handles failures for individual tables and reports partial failures if
      some tables fail.
    Args:
        event (dict): AWS Lambda event data. (Not used directly in this
            function.)
        context (object): AWS Lambda context object. (Not used directly in this
            function.)
    Returns:
        dict: A dictionary indicating the ingestion status.
    Logs:
        Info: When a table has not been updated since the last ingestion and
            every time a table has been succesfully ingested into the S3
            bucket.
        Exception: If tables failed to be written into the S3 bucket.
    """
    logger.info("Ingestion lambda invoked, started data ingestion")
    tables = util.fetch_tables(TABLES)
    failed_tables = []
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    for table_name, table_data in tables.items():
        prefix_time = f"{year}/{month}/{day}/{table_name}_{timestamp}"
        object_key = f"ingestion/{table_name}/{prefix_time}.json"
        try:
            if not table_data:
                logger.info(f"Table {table_name} has not been updated")
                continue
            s3_client.put_object(
                Bucket=S3_INGESTION_BUCKET,
                Key=object_key,
                # default str important for json serialisation
                Body=json.dumps(table_data, default=str),
            )
            logger.info(
                f"Successfully wrote {table_name} data to S3 key: {object_key}"
            )
        except Exception:
            failed_tables.append(table_name)
            logger.error(
                f"Failed to write {table_name} data to S3", exc_info=True
            )
            # raise err
            # raising error here could cause a failure that halts it
    if not failed_tables:
        return {
            "status": "Success",
            "message": "All data ingested successfully",
        }
    else:
        return {
            "status": "Partial Failure",
            "message": "Some tables failed to ingest",
            "failed_tables": failed_tables,
        }
