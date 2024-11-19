import os
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3", region_name="eu-west-2")


def lambda_handler(event, context):
    PROCESSED_BUCKET_NAME = os.environ.get("PROCESSED_BUCKET_NAME")
    FILE_LIST_KEY = os.environ.get("FILE_LIST_KEY")
    try:
        response = s3_client.get_object(
            Bucket=PROCESSED_BUCKET_NAME, Key=FILE_LIST_KEY
        )
        content = response["Body"].read()
        json_content = json.loads(content)

        file_paths = json_content.get("files", [])

        logger.info(f"Read file list: {file_paths}")

        return {"status": "Success", "file_paths": file_paths}

    except Exception as e:
        logger.error(f"Error reading file list: {e}", exc_info=True)
        return {"status": "Error", "message": str(e)}
