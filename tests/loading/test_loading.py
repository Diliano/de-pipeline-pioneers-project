from src.loading.loading import lambda_handler
import pytest
from moto import mock_aws
import boto3
from unittest.mock import patch
import os
import pandas as pd
import json


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


@pytest.fixture(scope="function")
def mock_secrets_manager(aws_credentials):
    """Mocked AWS Boto3 Secretsmanager client"""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture
def mock_db_credentials():
    """Mocked database credentials"""
    return {
        "USER": "db_user",
        "PASSWORD": "db_password",
        "DATABASE": "db_name",
        "HOST": "db_host",
        "PORT": 5432,
    }


@pytest.fixture
def mock_tables_data_frames():
    """Mocked tables data frames"""
    return {
        "dim_staff": pd.DataFrame(
            {
                "staff_id": [1, 2],
                "first_name": ["North", "Pipeline"],
                "last_name": ["Coder", "Pioneer"],
            }
        ),
        "fact_sales_order": pd.DataFrame(
            {
                "sales_order_id": [100, 101],
                "units_sold": [10, 30],
                "unit_price": [15.25, 42.30],
            }
        ),
    }


class TestLambdaHandler:
    @patch("src.loading.loading.read_file_list")
    @patch("src.loading.loading.process_parquet_files")
    @patch("src.loading.loading.connect_to_db")
    @patch("src.loading.loading.load_data_into_warehouse")
    def test_lambda_handler_loads_all_data_successfully(
        self,
        mock_load_data,
        mock_connect,
        mock_process_parquet,
        mock_read_file,
        mock_s3,
        mock_processed_bucket,
        mock_secrets_manager,
        mock_db_credentials,
        mock_tables_data_frames,
        caplog,
    ):
        # Arrange
        mock_read_file.return_value = [
            f"s3://{mock_processed_bucket}/dim_staff/dim_staff.parquet",
            f"s3://{mock_processed_bucket}/fact_sales_order/fact_s_o.parquet",
        ]
        mock_process_parquet.return_value = mock_tables_data_frames
        mock_load_data.return_value = {
            "successfully_loaded": ["dim_staff", "fact_sales_order"],
            "failed_to_load": [],
            "skipped_empty": [],
        }

        secret_name = "test_db_secret"
        mock_secrets_manager.create_secret(
            Name=secret_name,
            SecretString=json.dumps(mock_db_credentials),
        )
        # Act
        result = lambda_handler({}, None)
        # Assert
        assert result["status"] == "Success"
        assert (
            result["message"]
            == "All data loaded successfully into the warehouse."
        )
        assert result["results"]["successfully_loaded"] == [
            "dim_staff",
            "fact_sales_order",
        ]
        assert not result["results"]["failed_to_load"]
        assert not result["results"]["skipped_empty"]

        assert "All Parquet files processed successfully." in caplog.text
        assert (
            "All data loaded successfully into the warehouse." in caplog.text
        )

    @patch("src.loading.loading.read_file_list")
    def test_handles_no_file_paths(
        self, mock_read_file, mock_s3, mock_processed_bucket, caplog
    ):
        # Arrange
        mock_read_file.return_value = []
        # Act
        result = lambda_handler({}, None)
        # Assert
        assert result["status"] == "Success"
        assert result["message"] == "No files to process this time."
        assert "No files to process this time." in caplog.text

    @patch("src.loading.loading.read_file_list")
    @patch("src.loading.loading.process_parquet_files")
    def test_handles_no_data_frames(
        self,
        mock_process_parquet,
        mock_read_file,
        mock_s3,
        mock_processed_bucket,
        caplog,
    ):
        # Arrange
        mock_read_file.return_value = [
            f"s3://{mock_processed_bucket}/dim_staff/dim_staff.parquet"
        ]
        mock_process_parquet.return_value = {}
        # Act
        result = lambda_handler({}, None)
        # Assert
        assert result["status"] == "Failure"
        assert (
            result["message"]
            == "Failed to process any Parquet files. Check logs for details."
        )

    @patch("src.loading.loading.read_file_list")
    @patch("src.loading.loading.process_parquet_files")
    @patch("src.loading.loading.connect_to_db")
    @patch("src.loading.loading.load_data_into_warehouse")
    def test_handles_partial_success(
        self,
        mock_load_data,
        mock_connect,
        mock_process_parquet,
        mock_read_file,
        mock_s3,
        mock_processed_bucket,
        caplog,
    ):
        # Arrange
        mock_read_file.return_value = [
            f"s3://{mock_processed_bucket}/dim_staff/dim_staff.parquet",
            f"s3://{mock_processed_bucket}/dim_currency/dim_currency.parquet",
        ]
        mock_process_parquet.return_value = {
            "dim_staff": pd.DataFrame({"first_name": ["North"]}),
        }
        mock_load_data.return_value = {
            "successfully_loaded": ["dim_staff"],
            "failed_to_load": ["dim_currency"],
            "skipped_empty": [],
        }
        # Act
        result = lambda_handler({}, None)
        # Assert
        assert result["status"] == "Partial"
        assert result["message"] == (
            "Partial success loading data into the warehouse. "
            "Check logs for details."
        )
        assert result["results"]["successfully_loaded"] == ["dim_staff"]
        assert result["results"]["failed_to_load"] == ["dim_currency"]
        assert not result["results"]["skipped_empty"]

        assert (
            "Partial success: Some Parquet files could not be processed. "
            "Check logs for details."
        ) in caplog.text
        assert (
            "Partial success in loading data into the warehouse."
            in caplog.text
        )

    @patch("src.loading.loading.read_file_list")
    @patch("src.loading.loading.process_parquet_files")
    @patch("src.loading.loading.connect_to_db")
    @patch("src.loading.loading.load_data_into_warehouse")
    def test_handles_full_failure_in_warehouse_loading(
        self,
        mock_load_data,
        mock_connect,
        mock_process_parquet,
        mock_read_file,
        mock_s3,
        mock_processed_bucket,
        caplog,
    ):
        # Arrange
        mock_read_file.return_value = [
            f"s3://{mock_processed_bucket}/dim_staff/dim_staff.parquet",
        ]
        mock_process_parquet.return_value = {
            "dim_staff": pd.DataFrame({"first_name": ["North"]}),
        }
        mock_load_data.return_value = {
            "successfully_loaded": [],
            "failed_to_load": ["dim_staff"],
            "skipped_empty": [],
        }
        # Act
        result = lambda_handler({}, None)
        # Assert
        assert result["status"] == "Failure"
        assert result["message"] == (
            "Failed to load any data into the warehouse. "
            "Check logs for details."
        )
        assert "Failure in loading data into the warehouse." in caplog.text

    @patch("src.loading.loading.read_file_list")
    def test_handles_general_exceptions(self, mock_read_file, caplog):
        # Arrange
        mock_read_file.side_effect = Exception("Unexpected Error")
        # Act
        result = lambda_handler({}, None)
        # Assert
        assert result["status"] == "Failure"
        assert (
            result["message"]
            == "Lambda execution failed. Check logs for details."
        )
        assert "Lambda execution failed" in caplog.text
