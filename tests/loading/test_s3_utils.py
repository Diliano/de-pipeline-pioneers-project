from src.loading.code.s3_utils import read_file_list, process_parquet_files
import pytest
from moto import mock_aws
import boto3
import json
import os
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO


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


@pytest.fixture(scope="function")
def mock_processed_bucket(mock_s3):
    bucket_name = "processed-bucket"
    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return bucket_name


class TestReadFileList:
    def test_read_file_list(self, mock_s3, mock_processed_bucket, caplog):
        # Arrange
        json_key = "processed/file_list.json"
        json_content = {
            "files": [
                "s3://processed-bucket/table1/file1.parquet",
                "s3://processed-bucket/table2/file2.parquet",
            ]
        }

        mock_s3.put_object(
            Bucket=mock_processed_bucket,
            Key=json_key,
            Body=json.dumps(json_content),
        )
        # Act
        file_paths = read_file_list(mock_s3, mock_processed_bucket, json_key)
        # Assert
        assert file_paths == json_content["files"]
        assert "Read file list" in caplog.text

    def test_exception_given_missing_bucket(self, mock_s3, caplog):
        # Arrange
        bucket_name = "nonexistent-bucket"
        json_key = "file_list.json"
        # Act + Assert
        with pytest.raises(ClientError) as excinfo:
            read_file_list(mock_s3, bucket_name, json_key)

        assert "NoSuchBucket" in str(excinfo.value)
        assert "Error reading file list from S3" in caplog.text

    def test_exception_given_missing_key(
        self, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        json_key = "missing_file_list.json"

        # Act + Assert
        with pytest.raises(ClientError) as excinfo:
            read_file_list(mock_s3, mock_processed_bucket, json_key)

        assert "NoSuchKey" in str(excinfo.value)
        assert "Error reading file list from S3" in caplog.text

    def test_handles_non_clienterror_exceptions(
        self, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        json_key = "file_list.json"
        invalid_json_content = "invalid-json-content"  # invalid JSON

        mock_s3.put_object(
            Bucket=mock_processed_bucket,
            Key=json_key,
            Body=invalid_json_content,
        )
        # Act + Assert
        with pytest.raises(Exception):
            read_file_list(mock_s3, mock_processed_bucket, json_key)

        assert "Error reading file list" in caplog.text


class TestProcessParquetFiles:
    def test_process_parquet_files(self, mock_s3, mock_processed_bucket):
        # Arrange
        parquet_key1 = "table1/file1.parquet"
        parquet_key2 = "table2/file2.parquet"
        file_paths = [
            f"s3://{mock_processed_bucket}/{parquet_key1}",
            f"s3://{mock_processed_bucket}/{parquet_key2}",
        ]

        df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        df2 = pd.DataFrame({"col3": [3, 4], "col4": ["c", "d"]})

        buffer1 = BytesIO()
        buffer2 = BytesIO()
        df1.to_parquet(buffer1, index=False)
        df2.to_parquet(buffer2, index=False)
        buffer1.seek(0)
        buffer2.seek(0)

        mock_s3.put_object(
            Bucket=mock_processed_bucket,
            Key=parquet_key1,
            Body=buffer1.getvalue(),
        )
        mock_s3.put_object(
            Bucket=mock_processed_bucket,
            Key=parquet_key2,
            Body=buffer2.getvalue(),
        )
        # Act
        data_frames = process_parquet_files(mock_s3, file_paths)
        # Assert
        assert len(data_frames) == 2
        pd.testing.assert_frame_equal(data_frames[0], df1)
        pd.testing.assert_frame_equal(data_frames[1], df2)
