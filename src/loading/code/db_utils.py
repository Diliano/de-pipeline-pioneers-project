import boto3
import json
import logging
from botocore.exceptions import ClientError
from pg8000.native import Connection


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def retrieve_db_credentials(secret_name, region_name):
    try:
        secrets_manager_client = boto3.client(
            "secretsmanager", region_name=region_name
        )
        secret_value = secrets_manager_client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(secret_value["SecretString"])
        return secret
    except ClientError as e:
        logger.error(
            f"Error accessing Secrets Manager: {e}",
            exc_info=True,
        )
        raise
    except Exception as err:
        logger.error(f"Error retrieving DB credentials: {err}", exc_info=True)
        raise


def connect_to_db(secret_name, region_name):
    try:
        creds = retrieve_db_credentials(secret_name, region_name)
        conn = Connection(
            user=creds["USER"],
            password=creds["PASSWORD"],
            database=creds["DATABASE"],
            host=creds["HOST"],
            port=int(creds["PORT"]),
        )
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}", exc_info=True)
        raise
