from src.loading.loading_utils import read_file_list, process_parquet_files
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
    """Mocked AWS Boto3 S3 client"""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def mock_processed_bucket(mock_s3):
    """Mocked AWS S3 bucket, created with Boto3"""
    bucket_name = "processed-bucket"
    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return bucket_name


class TestReadFileList:
    def test_successfully_reads_file_list(
        self, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        json_key = f"{mock_processed_bucket}/file_list.json"
        json_content = {
            "files": [
                f"s3://{mock_processed_bucket}/table1/file1.parquet",
                f"s3://{mock_processed_bucket}/table2/file2.parquet",
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

    def test_clienterror_given_missing_bucket(self, mock_s3, caplog):
        # Arrange
        bucket_name = "nonexistent-bucket"
        json_key = "file_list.json"
        # Act + Assert
        with pytest.raises(ClientError) as excinfo:
            read_file_list(mock_s3, bucket_name, json_key)

        assert "NoSuchBucket" in str(excinfo.value)
        assert "Error reading file list from S3" in caplog.text

    def test_clienterror_given_missing_key(
        self, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        json_key = "missing_file_list.json"

        # Act + Assert
        with pytest.raises(ClientError) as excinfo:
            read_file_list(mock_s3, mock_processed_bucket, json_key)

        assert "NoSuchKey" in str(excinfo.value)
        assert "Error reading file list from S3" in caplog.text

    def test_handles_general_exceptions(
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
    def test_successfully_process_parquet_files(
        self, mock_s3, mock_processed_bucket
    ):
        # Arrange
        parquet_key1 = "processed/table1/file1.parquet"
        parquet_key2 = "processed/table2/file2.parquet"
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
        tables_data_frames = process_parquet_files(mock_s3, file_paths)
        # Assert
        assert len(tables_data_frames) == 2
        pd.testing.assert_frame_equal(tables_data_frames["table1"], df1)
        pd.testing.assert_frame_equal(tables_data_frames["table2"], df2)

    def test_clienterror_given_invalid_uri(self, mock_s3, caplog):
        # Arrange
        invalid_uri = "invalid-uri"
        # Act
        tables_data_frames = process_parquet_files(mock_s3, [invalid_uri])
        # Assert
        assert len(tables_data_frames) == 0
        assert f"Invalid S3 URI: {invalid_uri}" in caplog.text

    def test_clienterror_given_missing_bucket(self, mock_s3, caplog):
        # Arrange
        file_path = "s3://nonexistent-bucket/processed/table1/file1.parquet"
        # Act
        tables_data_frames = process_parquet_files(mock_s3, [file_path])
        # Assert
        assert len(tables_data_frames) == 0
        assert "Error accessing Parquet file from S3" in caplog.text
        assert "NoSuchBucket" in caplog.text

    def test_clienterror_given_missing_key(
        self, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        file_path = (
            f"s3://{mock_processed_bucket}/processed/"
            f"table1/missing_file.parquet"
        )
        # Act
        tables_data_frames = process_parquet_files(mock_s3, [file_path])
        # Assert
        assert len(tables_data_frames) == 0
        assert "Error accessing Parquet file from S3" in caplog.text
        assert "NoSuchKey" in caplog.text

    def test_handles_general_exceptions(
        self, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        file_path = (
            f"s3://{mock_processed_bucket}/processed/"
            "table1/corrupted_file.parquet"
        )
        corrupted_content = b"not-a-parquet-file"

        mock_s3.put_object(
            Bucket=mock_processed_bucket,
            Key="processed/table1/corrupted_file.parquet",
            Body=corrupted_content,
        )
        # Act
        tables_data_frames = process_parquet_files(mock_s3, [file_path])
        # Assert
        assert len(tables_data_frames) == 0
        assert "Error processing file" in caplog.text
