import pandas as pd
from unittest.mock import patch
from src.transformation.transformationutil import transform_dim_department


@patch("src.transformation.transformationutil.logger")
def test_transform_dim_department_valid_list(mock_logger):
    # Input: List of dictionaries
    input_data = [
        {
            "department_id": 1,
            "department_name": "HR",
            "location": "HQ",
            "manager": "Alice",
            "created_at": "2023-01-01",
            "last_updated": "2023-01-10",
        },
        {
            "department_id": 2,
            "department_name": "Engineering",
            "location": "HQ",
            "manager": "Bob",
            "created_at": "2023-01-01",
            "last_updated": "2023-01-10",
        },
    ]

    expected_output = pd.DataFrame(
        {
            "department_id": [1, 2],
            "department_name": ["HR", "Engineering"],
            "location": ["HQ", "HQ"],
            "manager": ["Alice", "Bob"],
        }
    )

    result = transform_dim_department(input_data)

    # Asserting the result matches expected DataFrame
    pd.testing.assert_frame_equal(result, expected_output)
    mock_logger.error.assert_not_called()


@patch("src.transformation.transformationutil.logger")
def test_transform_dim_department_valid_dataframe(mock_logger):
    # Input: DataFrame
    input_data = pd.DataFrame(
        {
            "department_id": [1, 2],
            "department_name": ["HR", "Engineering"],
            "location": ["HQ", "HQ"],
            "manager": ["Alice", "Bob"],
            "created_at": ["2023-01-01", "2023-01-01"],
            "last_updated": ["2023-01-10", "2023-01-10"],
        }
    )

    expected_output = pd.DataFrame(
        {
            "department_id": [1, 2],
            "department_name": ["HR", "Engineering"],
            "location": ["HQ", "HQ"],
            "manager": ["Alice", "Bob"],
        }
    )

    result = transform_dim_department(input_data)

    # Asserting the result matches expected DataFrame
    pd.testing.assert_frame_equal(result, expected_output)
    mock_logger.error.assert_not_called()


@patch("src.transformation.transformationutil.logger")
def test_transform_dim_department_missing_columns(mock_logger):
    # Input: Missing required columns
    input_data = [
        {
            "department_id": 1,
            "department_name": "HR",
            "created_at": "2023-01-01",
        }
    ]

    result = transform_dim_department(input_data)

    assert result is None

    # Verifying logger captured the error
    mock_logger.error.assert_called_once()


@patch("src.transformation.transformationutil.logger")
def test_transform_dim_department_empty_input(mock_logger):
    # Input: Empty data
    input_data = []

    expected_output = pd.DataFrame(
        columns=["department_id", "department_name", "location", "manager"]
    )

    result = transform_dim_department(input_data)

    # Asserting the result matches an empty DataFrame
    pd.testing.assert_frame_equal(result, expected_output)
    mock_logger.error.assert_not_called()


@patch("src.transformation.transformationutil.logger")
def test_transform_dim_department_invalid_input_type(mock_logger):
    # Input: Invalid data type
    input_data = "invalid_data"

    result = transform_dim_department(input_data)

    assert result is None

    print(mock_logger.text)
    # Verifying logger captured the error
    mock_logger.error.assert_called_once_with(
        "Validation error: DataFrame constructor not properly called!"
    )
