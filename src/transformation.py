"""
def lambda_handler

"""

import boto3
import pandas as pd
import json
import logging



s3_client = boto3.client("s3")

def read_data_from_s3(bucket_name, prefix, client):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    dataframes = []

    

