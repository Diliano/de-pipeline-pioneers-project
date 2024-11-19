from src.loading.code.utils import read_file_list
import pytest
from moto import mock_aws
import boto3
import json
import os
from botocore.exceptions import ClientError


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


def test_read_file_list(mock_s3, caplog):
    # Arrange
    bucket_name = "processed-bucket"
    json_key = "processed/file_list.json"
    json_content = {
        "files": [
            "s3://processed-bucket/table1/file1.parquet",
            "s3://processed-bucket/table2/file2.parquet",
        ]
    }

    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_s3.put_object(
        Bucket=bucket_name, Key=json_key, Body=json.dumps(json_content)
    )
    # Act
    file_paths = read_file_list(mock_s3, bucket_name, json_key)
    # Assert
    assert file_paths == json_content["files"]
    assert "Read file list" in caplog.text


def test_exception_given_missing_bucket(mock_s3, caplog):
    # Arrange
    bucket_name = "nonexistent-bucket"
    json_key = "file_list.json"
    # Act + Assert
    with pytest.raises(ClientError) as excinfo:
        read_file_list(mock_s3, bucket_name, json_key)

    assert "NoSuchBucket" in str(excinfo.value)
    assert "Error reading file list from S3" in caplog.text


def test_exception_given_missing_key(mock_s3, caplog):
    # Arrange
    bucket_name = "processed-bucket"
    json_key = "missing_file_list.json"

    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    # Act + Assert
    with pytest.raises(ClientError) as excinfo:
        read_file_list(mock_s3, bucket_name, json_key)

    assert "NoSuchKey" in str(excinfo.value)
    assert "Error reading file list from S3" in caplog.text


def test_handles_non_clienterror_exceptions(mock_s3, caplog):
    # Arrange
    bucket_name = "processed-bucket"
    json_key = "file_list.json"
    invalid_json_content = "invalid-json-content"  # invalid JSON

    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_s3.put_object(
        Bucket=bucket_name, Key=json_key, Body=invalid_json_content
    )
    # Act + Assert
    with pytest.raises(Exception):
        read_file_list(mock_s3, bucket_name, json_key)

    assert "Unexpected error" in caplog.text
