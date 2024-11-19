from src.loading.code.loading import lambda_handler
import pytest
from moto import mock_aws
import boto3
import json
import os


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


def test_loading_lambda_reads_file_list(mock_s3):
    # Arrange
    bucket_name = "processed-bucket"
    json_key = "processed/file_list.json"
    json_content = {
        "files": [
            "s3://processed-bucket/table1/2023/11/19/file1.parquet",
            "s3://processed-bucket/table2/2023/11/19/file2.parquet",
        ]
    }

    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_s3.put_object(
        Bucket=bucket_name, Key=json_key, Body=json.dumps(json_content)
    )

    os.environ["PROCESSED_BUCKET_NAME"] = bucket_name
    os.environ["FILE_LIST_KEY"] = json_key
    # Act
    result = lambda_handler({}, None)
    # Assert
    assert result["status"] == "Success"
    assert result["file_paths"] == json_content["files"]
