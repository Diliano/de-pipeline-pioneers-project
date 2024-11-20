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
    """
    Retrieve database credentials from AWS Secrets Manager.
    Args:
        secrets_manager_client (boto3.client): A boto3 Secrets Manager client.
    Returns:
        dict: A dictionary containing database credentials with keys:
            "USER", "PASSWORD", "DATABASE", "HOST", and "PORT".
    Logs:
        Exception: If there is an issue retrieving the secret from Secrets
        Manager.
    """
    try:
        secret = secrets_manager_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret["SecretString"])
        return secret
    except Exception as err:
        logger.error(f"Unexpected error occurred {err}", exc_info=True)
        # raise err


def connect_to_db():
    """
    Establish a connection to the database using credentials from AWS Secrets
    Manager.
    Returns:
        pg8000.native.Connection: An active database connection object.
    Logs:
        Exception: If the database connection fails.
    """
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
    """
    Retrieve the last ingestion timestamp from an S3 bucket.
    This function reads a JSON object stored in an S3 bucket at a specified
    key.
    It extracts the `timestamp` field and converts it into a `datetime`
    object.
    Returns:
        datetime.datetime: The timestamp of the last ingestion if available.
        str: The default timestamp string "1970-01-01 00:00:00" if no
            timestamp is found or there is a botocore ClientError.
    Logs:
        Exception: For any unexpected errors during the process.
    """
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
    """
    Update the last ingestion timestamp in an S3 bucket.
    This function generates the current timestamp in ISO 8601 format
    and uploads it as a JSON object to the specified S3 bucket and key.
    The S3 object will have the structure:
        {
            "timestamp": "<current ISO 8601 timestamp>"
        }
    """
    current_timestamp = datetime.now().isoformat()
    print(S3_INGESTION_BUCKET)
    s3_client.put_object(
        Bucket=S3_INGESTION_BUCKET,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}),
    )


def fetch_tables(tables: list = TABLES):
    """
    Fetch data from specified tables in the database updated since last
    ingestion.
    Args:
        tables (list): List of table names to fetch data from. Defaults to the
            `TABLES` constant.
    Returns:
        dict: A dictionary where keys are table names and values are lists of
            row data as dictionaries.
    Logs:
        Info: Every time a table is fetched succesfully.
        Exception: If database connection or query execution fails.
    """
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
