from src.loading.code.db_utils import (
    retrieve_db_credentials,
    connect_to_db,
    load_data_into_warehouse,
)
import pytest
from moto import mock_aws
import boto3
import json
import os
from botocore.exceptions import ClientError
from unittest.mock import patch
import pandas as pd


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_secrets_manager(aws_credentials):
    """Mocked AWS Boto3 Secretsmanager client"""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture(scope="function")
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
    """
    Mocked tables data frames
    """
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
        "dim_location": pd.DataFrame(),
    }


class TestRetrieveDbCredentials:
    def test_successfully_retrieves_db_credentials(
        self, mock_secrets_manager, mock_db_credentials
    ):
        # Arrange
        secret_name = "db_secret"
        region_name = "eu-west-2"

        mock_secrets_manager.create_secret(
            Name=secret_name,
            SecretString=json.dumps(mock_db_credentials),
        )
        # Act
        credentials = retrieve_db_credentials(secret_name, region_name)
        # Assert
        assert credentials == mock_db_credentials

    def test_clienterror_given_missing_secret(
        self, mock_secrets_manager, caplog
    ):
        # Arrange
        secret_name = "nonexistent_secret"
        region_name = "eu-west-2"
        # Act + Assert
        with pytest.raises(ClientError) as excinfo:
            retrieve_db_credentials(secret_name, region_name)

        assert "ResourceNotFound" in str(excinfo.value)
        assert "Error accessing Secrets Manager" in caplog.text

    def test_handles_general_exceptions(self, mock_secrets_manager, caplog):
        # Arrange
        secret_name = "db_secret"
        region_name = "eu-west-2"
        invalid_secret_string = "not-a-json"

        mock_secrets_manager.create_secret(
            Name=secret_name,
            SecretString=invalid_secret_string,
        )
        # Act + Assert
        with pytest.raises(Exception):
            retrieve_db_credentials(secret_name, region_name)

        assert "Error retrieving DB credentials" in caplog.text


class TestConnectToDb:
    @patch("src.loading.code.db_utils.Connection")
    @patch("src.loading.code.db_utils.retrieve_db_credentials")
    def test_successfully_connects_to_db(
        self, mock_retrieve_creds, mock_pg_connect, mock_db_credentials, caplog
    ):
        # Arrange
        secret_name = "my_db_secret"
        region_name = "eu-west-2"

        mock_retrieve_creds.return_value = mock_db_credentials
        mock_conn = mock_pg_connect.return_value
        # Act
        conn = connect_to_db(secret_name, region_name)
        # Assert
        assert conn == mock_conn
        assert "Successfully connected to the database" in caplog.text

        mock_retrieve_creds.assert_called_once_with(secret_name, region_name)
        mock_pg_connect.assert_called_once_with(
            user="db_user",
            password="db_password",
            database="db_name",
            host="db_host",
            port=5432,
        )

    @patch("src.loading.code.db_utils.Connection")
    @patch("src.loading.code.db_utils.retrieve_db_credentials")
    def test_connect_to_db_handles_exceptions(
        self, mock_retrieve_creds, mock_pg_connect, mock_db_credentials, caplog
    ):
        # Arrange
        secret_name = "my_db_secret"
        region_name = "eu-west-2"

        mock_retrieve_creds.return_value = mock_db_credentials
        mock_pg_connect.side_effect = Exception("Connection failed")
        # Act + Assert
        with pytest.raises(Exception) as excinfo:
            connect_to_db(secret_name, region_name)

        assert "Connection failed" in str(excinfo.value)
        assert "Error connecting to the database" in caplog.text


class TestLoadDataIntoWarehouse:
    @patch("src.loading.code.db_utils.Connection")
    def test_successfully_loads_data_into_warehouse(
        self, mock_pg_connect, mock_tables_data_frames, caplog
    ):
        # Arrange
        def normalise_query(sql):
            return (" ").join(sql.split())

        mock_conn = mock_pg_connect.return_value

        expected_dimension_query = normalise_query(
            """
            INSERT INTO "dim_staff" ("staff_id", "first_name", "last_name")
            VALUES (%s, %s, %s)
            ON CONFLICT ("staff_id") DO UPDATE
            SET "first_name" = EXCLUDED."first_name",
                "last_name" = EXCLUDED."last_name";
        """
        )

        expected_fact_query = normalise_query(
            """
            INSERT INTO "fact_sales_order" ("sales_order_id", "units_sold", "unit_price")
            VALUES (%s, %s, %s);
        """
        )
        # Act
        results = load_data_into_warehouse(mock_conn, mock_tables_data_frames)

        calls = mock_conn.run.call_args_list
        executed_queries = [
            (normalise_query(call.kwargs["sql"]), call.kwargs["params"])
            for call in calls
        ]
        # Assert
        assert results["successfully_loaded"] == [
            "dim_staff",
            "fact_sales_order",
        ]
        assert results["failed_to_load"] == []
        assert results["skipped_empty"] == ["dim_location"]

        assert "Skipping dim_location: DataFrame is empty" in caplog.text
        assert "Successfully loaded data into 'dim_staff" in caplog.text
        assert "Successfully loaded data into 'fact_sales_order" in caplog.text

        assert (
            expected_dimension_query,
            [
                (1, "North", "Coder"),
                (2, "Pipeline", "Pioneer"),
            ],
        ) in executed_queries

        assert (
            expected_fact_query,
            [
                (100, 10, 15.25),
                (101, 30, 42.30),
            ],
        ) in executed_queries

    @patch("src.loading.code.db_utils.Connection")
    def test_handles_exceptions(
        self, mock_pg_connect, mock_tables_data_frames, caplog
    ):
        # Arrange
        mock_conn = mock_pg_connect.return_value

        mock_conn.run.side_effect = Exception("SQL execution failed")
        # Act
        results = load_data_into_warehouse(mock_conn, mock_tables_data_frames)
        # Assert
        assert results["successfully_loaded"] == []
        assert results["failed_to_load"] == ["dim_staff", "fact_sales_order"]
        assert results["skipped_empty"] == ["dim_location"]

        assert "Error loading data into 'dim_staff'" in caplog.text
        assert "Error loading data into 'fact_sales_order" in caplog.text
