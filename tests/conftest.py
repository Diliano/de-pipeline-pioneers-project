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
def valid_sales_order_data():
    """Fixture providing valid sales_order data."""
    return [
        {
            "sales_order_id": 1,
            "created_at": "2023-01-01 12:34:56",
            "last_updated": "2023-01-02 14:45:30",
            "staff_id": 101,
            "counterparty_id": 201,
            "design_id": 301,
            "units_sold": 50,
            "unit_price": "25.75",
            "currency_id": 1,
            "agreed_payment_date": "2023-01-15",
            "agreed_delivery_date": "2023-01-20",
            "agreed_delivery_location_id": 401,
        }
    ]


@pytest.fixture
def invalid_sales_order_data():
    """Fixture providing invalid sales_order data."""
    return [
        {
            "sales_order_id": 1,
            "created_at": "not-a-date",
            "last_updated": "not-a-date",
            "staff_id": "invalid",
            "units_sold": "invalid",
            "unit_price": "invalid",
        }
    ]


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
