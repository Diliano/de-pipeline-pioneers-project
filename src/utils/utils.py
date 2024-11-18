from src.ingestion import (
    logger,
)
from botocore.exceptions import ClientError
from pg8000.exceptions import DatabaseError
from pg8000.native import Connection
from datetime import datetime
import json

from src.ingestion import (
    s3_client,
    secrets_manager_client,
    SECRET_NAME,
    TABLES,
    TIMESTAMP_FILE_KEY,
    S3_INGESTION_BUCKET,
)


def retrieve_db_credentials(secrets_manager_client):
    try:
        secret = secrets_manager_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret["SecretString"])
        return secret
    except Exception as err:
        logger.error(f"Unexpected error occurred {err}", exc_info=True)
        # raise err


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
        logger.error(f"Database connection failed: {e}", exc_info=True)
        # raise e


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
        # raise


def update_last_ingestion_timestamp():
    current_timestamp = datetime.now().isoformat()
    print(S3_INGESTION_BUCKET)
    s3_client.put_object(
        Bucket=S3_INGESTION_BUCKET,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}),
    )


def fetch_tables(tables: list = TABLES):
    tables_data = {}
    try:
        last_ingestion_timestamp = get_last_ingestion_timestamp()
        with connect_to_db() as db:
            for table_name in tables:
                query = (
                    f"SELECT * FROM {table_name}"  # nosec B608
                    + " WHERE last_updated > :s;"  # nosec B608
                )
                logger.debug(f"Query for {table_name}: {query}")
                try:
                    rows = db.run(
                        query,
                        s=last_ingestion_timestamp,
                    )
                    if rows:
                        column = [col["name"] for col in db.columns]
                        tables_data[table_name] = [
                            dict(zip(column, row)) for row in rows
                        ]
                        logger.info(
                            f"Fetched new data from {table_name} successfully."
                        )
                    else:
                        logger.info(f"No new data in {table_name}")
                except DatabaseError:
                    logger.error(
                        f"Database error, fetching data {table_name}",
                        exc_info=True,
                    )
                except Exception:
                    logger.error(
                        f"Failed to fetch data from {table_name}",
                        exc_info=True,
                    )
        update_last_ingestion_timestamp()
        return tables_data

    except Exception as err:
        logger.error(f"Database connection failed: {err}", exc_info=True)
        # raise err
