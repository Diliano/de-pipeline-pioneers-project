import boto3
import json
import logging
from botocore.exceptions import ClientError


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
