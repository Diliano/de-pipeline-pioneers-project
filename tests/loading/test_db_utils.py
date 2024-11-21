from src.loading.code.db_utils import retrieve_db_credentials, connect_to_db
import pytest
from moto import mock_aws
import boto3
import json
import os
from botocore.exceptions import ClientError
from unittest.mock import patch


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
    """Mocked AWS Secretsmanager"""
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


@patch("src.loading.code.db_utils.Connection")
@patch("src.loading.code.db_utils.retrieve_db_credentials")
def test_successfully_connects_to_db(
    mock_retrieve_creds, mock_pg_connect, mock_db_credentials
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
    mock_retrieve_creds, mock_pg_connect, mock_db_credentials, caplog
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
