import boto3
import json
import logging
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_file_list(s3_client, bucket_name, key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read()
        json_content = json.loads(content)
        file_paths = json_content.get("files", [])
        logger.info(f"Read file list: {file_paths}")
        return file_paths
    except ClientError as e:
        logger.error(f"Error reading file list from S3: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise
