import boto3
import json
import logging
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO
import re


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
        logger.error(f"Error reading file list: {e}", exc_info=True)
        raise


def process_parquet_files(s3_client, file_paths):
    data_frames = []
    for file_path in file_paths:
        try:
            match = re.match(r"s3://([^/]+)/(.+)", file_path)
            if not match:
                logger.error(f"Invalid S3 URI: {file_path}")
                continue
            bucket_name, key = match.groups()

            obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            parquet_content = obj["Body"].read()
            buffer = BytesIO(parquet_content)

            df = pd.read_parquet(buffer)
            data_frames.append(df)

            logger.info(f"Read Parquet file: {file_path}")
        except ClientError as e:
            logger.error(
                f"Error accessing Parquet file from S3: {file_path}: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Error processing file: {file_path}: {e}", exc_info=True
            )
            continue
    return data_frames
