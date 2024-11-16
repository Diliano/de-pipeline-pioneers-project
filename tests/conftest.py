from moto import mock_aws
import boto3
import pytest
import json

#
from src.ingestion import (
    SECRET_NAME,
)


# Defining a fixture
@pytest.fixture
def mock_secrets_manager():
    with mock_aws():
        secrets_client = boto3.client(
            "secretsmanager",
            region_name="eu-west-2"
        )

        # Create a mock secret
        secrets_client.create_secret(
            Name=SECRET_NAME,
            SecretString=json.dumps(
                {
                    "HOST": "test-host",
                    "USER": "test-user",
                    "PASSWORD": "test-password",
                    "DATABASE": "test-db",
                    "PORT": "5432",
                }
            ),
        )

        yield secrets_client


@pytest.fixture
def sample_table_data():
    # Samples data
    return {
        "table1": [{"id": 1, "value": "test1"}],
        "table2": [{"id": 2, "value": "test2"}],
    }