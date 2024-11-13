# from botocore.exceptions import ClientError
from pg8000.native import Connection
from datetime import datetime, timedelta
import boto3
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
s3_client = boto3.client("s3")
secrets_manager_client = boto3.client("secretsmanager")
TIMESTAMP_FILE_KEY = "metadata/last_ingestion_timestamp.json"
BUCKET_NAME = os.getenv(
    "S3_BUCKET_NAME"
)  # MAKE SURE THIS IS DEFINED IN THE LAMBDA CODE FOR TF


def retrieve_db_credentials(secrets_manager_client):
    try:
        secret = secrets_manager_client.get_secret_value(
            SecretId="nc-totesys-db-credentials"
        )
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

        Connection(
            user=USER,
            database=DATABASE,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )

    except Exception as e:
        logger.error("Database connection failed", exc_info=True)
        raise e

def get_last_ingestion_timestamp():
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=TIMESTAMP_FILE_KEY)
        last_ingestion_data = json.loads(response['Body'].read().decode('utf-8'))
        return datetime.fromisoformat(last_ingestion_data['timestamp'])
    except s3_client.exceptions.NoSuchKey:
        return datetime.now() - timedelta(day=1)


def update_last_ingestion_timestamp():
    current_timestamp = datetime.now().isoformat()
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}))

def fetch_tables():

    table_names = [
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
    
    tables_data = {}

    try:
        last_ingestion_timestamp = get_last_ingestion_timestamp()

        with connect_to_db() as db:
            for table_name in table_names:
                query = f"SELECT * FROM {table_name} WHERE updated_at > %s;"
                try:
                    tables_data[table_name] = db.run(query, (last_ingestion_timestamp,))
                    logger.info(f"Fetched new data from {table_name} successfully.")
                except Exception as err:
                    logger.error(f"Failed to fetch data from {table_name}", exc_info=True)
                    raise

        update_last_ingestion_timestamp()

        return tables_data

    except Exception as err:
        logger.error("Failed to fetch data from tables", exc_info=True)
        raise


def lambda_handler(event, context):
    tables = fetch_tables()
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    success = True
    for table_name, table_data in tables.items():
        object_key = f"{table_name}/{table_name}_{timestamp}.json"
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME, Key=object_key, Body=json.dumps(table_data)
            )
            logger.info(
                f"Successfully wrote {table_name} data to S3 key: {object_key}"
                )
        except Exception:
            success = False
            logger.error("Failed to write data to S3", exc_info=True)
            # raise err
    if success:
        return {
            "status": "Success", "message": "All data ingested successfully"
            }
    else:
        return {
            "status": "Partial Failure",
            "message": "Some tables failed to ingest"
            }
