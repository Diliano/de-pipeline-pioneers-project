from botocore.exceptions import ClientError
from pg8000.native import Connection
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
    "S3_BUCKET_NAME",
    "nc-pipeline-pioneers-ingestion20241112120531000200000003"
)  # MAKE SURE THIS IS DEFINED IN THE LAMBDA CODE FOR TF


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

s3_client = boto3.client("s3")
secrets_manager_client = boto3.client("secretsmanager")


def retrieve_db_credentials(secrets_manager_client):
    try:
        secret = secrets_manager_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret["SecretString"])
        return secret
    except Exception as err:
        logger.error(f"Unexpected error occurred {err}", exc_info=True)
        raise err


def connect_to_db():
    try:
        creds = retrieve_db_credentials(secrets_manager_client)
        USER = creds["USER"]
        PASSWORD = creds["PASSWORD"]
        DATABASE = creds["DATABASE"]
        HOST = creds["HOST"]
        PORT = creds["PORT"]

        return Connection(
            user=USER,
            database=DATABASE,
            password=PASSWORD,
            host=HOST,
            port=PORT,
        )

    except Exception as e:
        logger.error("Database connection failed", exc_info=True)
        raise e


def get_last_ingestion_timestamp():
    try:
        response = s3_client.get_object(
            Bucket=S3_INGESTION_BUCKET, Key=TIMESTAMP_FILE_KEY
        )
        # Reading content only if 'Body' exists and is not None
        body = response.get("Body", "")
        if body:
            last_ingestion_data = json.loads(body.read().decode("utf-8"))

            # Ensuring the 'timestamp' key exists in the json data
            timestamp_str = last_ingestion_data.get("timestamp", "")
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str)

        return "1970-01-01 00:00:00"
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            return "1970-01-01 00:00:00"
        elif e.response["Error"]["Code"] == "NoSuchKey":
            return "1970-01-01 00:00:00"
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        raise


def update_last_ingestion_timestamp():
    current_timestamp = datetime.now().isoformat()
    s3_client.put_object(
        Bucket=S3_INGESTION_BUCKET,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}),
    )


def fetch_tables():
    tables_data = {}
    try:
        last_ingestion_timestamp = get_last_ingestion_timestamp()
        with connect_to_db() as db:
            for table_name in TABLES:
                query = f"SELECT * FROM {table_name} WHERE last_updated > :s"
                try:
                    rows = db.run(query, s=last_ingestion_timestamp)

                    column = [col["name"] for col in db.columns]
                    tables_data[table_name] = [
                        dict(zip(column, row)) for row in rows
                    ]
                    logger.info(
                        f"Fetched new data from {table_name} successfully."
                    )
                except Exception:
                    logger.error(
                        f"Failed to fetch data from {table_name}",
                        exc_info=True,
                    )
                    raise
        update_last_ingestion_timestamp()
        return tables_data

    except Exception as err:
        logger.error("Database connection failed", exc_info=True)
        raise err
    finally:
        if db:
            db.close()
            logger.info("Database connection closed")


def lambda_handler(event, context):
    logger.info("Ingestion lambda invoked, started data ingestion")
    tables = fetch_tables()
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    success = True
    for table_name, table_data in tables.items():
        object_key = f"ingestion/{table_name}/{table_name}_{timestamp}.json"
        try:
            if not table_data:
                logger.info(f"Table {table_name} has not been updated")

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
            success = False
            logger.error("Failed to write data to S3", exc_info=True)
            # raise err
            # raising error here could cause a failure that halts it
    if success:
        return {
            "status": "Success",
            "message": "All data ingested successfully",
        }
    else:
        return {
            "status": "Partial Failure",
            "message": "Some tables failed to ingest",
        }
