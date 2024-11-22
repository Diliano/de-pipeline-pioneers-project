import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3", region_name="eu-west-2")


def lambda_handler(event, context):
    pass
