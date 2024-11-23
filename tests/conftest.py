from moto import mock_aws
import boto3
import pytest
import json

#
from src.ingestion.utils import SECRET_NAME

TEST_BUCKET = "test_ingestion_bucket"


# Defining a fixture
@pytest.fixture
def mock_secrets_manager():
    with mock_aws():
        secrets_client = boto3.client(
            "secretsmanager", region_name="eu-west-2"
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
def mock_s3_event():
    """Generates a mock S3 event."""

    def _event(key):
        return {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": TEST_BUCKET},
                        "object": {"key": key},
                    }
                }
            ]
        }

    return _event


@pytest.fixture
def mock_s3_client():
    # mocking s3 client
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        yield s3_client


@pytest.fixture
def sample_table_data():
    # Samples data
    return {
        "table1": [{"id": 1, "value": "test1"}],
        "table2": [{"id": 2, "value": "test1"}],
    }


@pytest.fixture
def expected_table_data():
    # expected fetch tables data
    return {
        "table1": [{"id": 1, "data": "value1"}, {"id": 2, "data": "value2"}],
        "table2": [{"id": 1, "data": "value1"}, {"id": 2, "data": "value2"}],
    }


@pytest.fixture
def mock_rows():
    # expected rows for fetch tables
    return [[1, "value1"], [2, "value2"]]


@pytest.fixture
def mock_tables():
    # expected tables for parameter for
    # fetch tables
    return ["table1", "table2"]


@pytest.fixture
def mock_columns():
    # expected returned columns
    # for fetch tables
    return [{"name": "id"}, {"name": "data"}]
