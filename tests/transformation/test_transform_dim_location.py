from src.transformation.transformationutil import transform_dim_location
from unittest.mock import patch
import pandas as pd


@patch("src.transformation.transformation.logger")
def test_transform_dim_location_valid_list(mock_logger):
    # Input: List of dictionaries
    address_data = [
        {
            "address_id": 1,
            "address_line_1": "123 Main St",
            "address_line_2": "Apt 4B",
            "district": "Central",
            "city": "Metropolis",
            "postal_code": "12345",
            "country": "Fictionland",
            "phone": "123-456-7890",
            "created_at": "2023-01-01",
            "last_updated": "2023-01-10",
        }
    ]

    expected_output = pd.DataFrame(
        {
            "location_id": [1],
            "address_line_1": ["123 Main St"],
            "address_line_2": ["Apt 4B"],
            "district": ["Central"],
            "city": ["Metropolis"],
            "postal_code": ["12345"],
            "country": ["Fictionland"],
            "phone": ["123-456-7890"],
        }
    )

    result = transform_dim_location(address_data)

    # Asserting the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result, expected_output)
    mock_logger.error.assert_not_called()


@patch("src.transformation.transformation.logger")
def test_transform_dim_location_empty_list(mock_logger):
    # Input: Empty list
    address_data = []

    transform_dim_location(address_data)

    # Asserting the result matches an empty DataFrame
    mock_logger.error.assert_called_once_with(
        "Validation error: Input must be populated."
    )


@patch("src.transformation.transformation.logger")
def test_transform_dim_location_invalid_input_type(mock_logger):
    # Input: Invalid type
    address_data = "invalid_data"

    result = transform_dim_location(address_data)

    # Asserting the function returns None
    assert result is None
    mock_logger.error.assert_called_once_with(
        "Validation error: Input must be a list of dictionaries."
    )


# Might be necessary to add column checks
