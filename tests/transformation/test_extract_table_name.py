from unittest.mock import patch
from src.transformation.transformationutil import extract_table_name



# @patch("src.transformation.transformationutil.logger")
def test_extract_table_name_missing_table_name():
    # Input: Invalid S3 key (missing table name)
    s3_key = "ingestion/"

    result = extract_table_name(s3_key)

    assert result == ''


@patch("src.transformation.transformationutil.logger")
def test_extract_table_name_valid_key(mock_logger):
    # Input: Valid S3 key
    s3_key = "ingestion/table_name/file_235235235235.json"

    expected_table_name = "table_name"

    result = extract_table_name(s3_key)

    # Asserting the result is correct
    assert result == expected_table_name
    mock_logger.error.assert_not_called()


@patch("src.transformation.transformationutil.logger")
def test_extract_table_name_empty_string(mock_logger):
    # Input: Empty string
    s3_key = ""

    result = extract_table_name(s3_key)

    assert result is None

    # Verifying logger captured the error, :)
    mock_logger.error.assert_called_once_with("Invalid S3 key format: : list index out of range")


@patch("src.transformation.transformationutil.logger")
def test_extract_table_name_invalid_input_type(mock_logger):
    # Input: Invalid input type (None)
    s3_key = None

    result = extract_table_name(s3_key)

    assert result is None

    # Verifying logger captured the error, :-()
    mock_logger.error.assert_called_once_with(f"Object {s3_key} has no attribute 'split'")