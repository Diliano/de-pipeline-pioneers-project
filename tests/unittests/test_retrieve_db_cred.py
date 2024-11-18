from moto import mock_aws
import boto3
# import pytest

from src.utils.utils import retrieve_db_credentials

# @pytest.mark.xfail
def test_retrieve_db_credentials_success(mock_secrets_manager, caplog):
    result = retrieve_db_credentials(mock_secrets_manager)

    expected_secret = {
        "HOST": "test-host",
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "PORT": "5432",
    }
    assert result == expected_secret
    assert (
        "Unexpected error occurred" not in caplog.text
    )  # Ensuring no error was logged


# @pytest.mark.xfail
def test_retrieve_db_credentials_error(caplog):
    with mock_aws():
        # Create the Secrets Manager client without creating the secret
        secrets_client = boto3.client(
            "secretsmanager", region_name="eu-west-2"
        )

        # with pytest.raises(Exception):
        #     retrieve_db_credentials(secrets_client)
        retrieve_db_credentials(secrets_client)
        assert "Unexpected error occurred" in caplog.text
