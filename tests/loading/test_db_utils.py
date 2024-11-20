from src.loading.code.db_utils import retrieve_db_credentials
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
def mock_secrets_manager(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


def test_successfully_retrieves_db_credentials(mock_secrets_manager):
    # Arrange
    secret_name = "db_secret"
    region_name = "eu-west-2"
    expected_credentials = {
        "USER": "db_user",
        "PASSWORD": "db_password",
        "DATABASE": "db_name",
        "HOST": "db_host",
        "PORT": "5432",
    }

    mock_secrets_manager.create_secret(
        Name=secret_name,
        SecretString=json.dumps(expected_credentials),
    )
    # Act
    credentials = retrieve_db_credentials(secret_name, region_name)
    # Assert
    assert credentials == expected_credentials
