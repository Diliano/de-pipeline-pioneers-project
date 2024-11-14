import logging
import boto3
import pandas as pd
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    logger.info("Transformation lambda triggered")

    bucket_name = 'nc-pipeline-pioneers-ingestion20241112120531000200000003'
    folder_name = 'counterparty/'

    read_data_from_s3(bucket_name, folder_name)

    return {"statusCode": 200, "body": "Transformation complete"}


def read_data_from_s3(bucket_name, folder_name):
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_name)

        logger.info(f"Checking for files in {folder_name}")

        if 'Contents' not in response:
            logger.info(f"No files found in folder: {folder_name}")
            return
        
        

        print(f"Files found:")
        for obj in response['Contents']:
            file_key = obj['Key']
            logger.info(f" - {file_key}")

        for obj in response['Contents']:
            file_key = obj['Key']
            try:
                response = s3_client.get_object(
                    Bucket=bucket_name, Key=file_key)
                data = json.loads(response['Body'].read().decode('utf-8'))

                df = pd.DataFrame(data)

                logger.info("---------------")
                logger.info(f"Data preview for {file_key}:")
                logger.info(f"{df.head()}")

                logger.info(f"Dataframe info:")
                logger.info(f"{df.info()}")

                logger.info("---------------")
    

            except json.JSONDecodeError:
                logger.error(
                    f"Error: The file {file_key} is not in valid JSON format.")

            except Exception as e:
                logger.error(f"Error reading {file_key}: {e}")

    except Exception as e:
        logger.error(f"Error accessing the S3 bucket: {e}")



# read_data_from_s3(
#     'nc-pipeline-pioneers-ingestion20241112120531000200000003', 'ingestion/counterparty/')
