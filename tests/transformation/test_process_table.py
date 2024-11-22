from unittest.mock import Mock, patch
from src.transformation.transformationutil import process_table
import pandas as pd


@patch("src.transformation.transformationutil.logger")
def test_process_table_valid_input(mock_logger):
    # Mock transformation function
    mock_transform_function = Mock()
    mock_transform_function.return_value = pd.DataFrame(
        {"column1": [1, 2], "column2": ["value1", "value2"]}
    )

    table_name = "test_table"
    data = [
        {"column1": 1, "column2": "value1"},
        {"column1": 2, "column2": "value2"},
    ]

    result = process_table(table_name, mock_transform_function, data)

    expected_output = pd.DataFrame(
        {"column1": [1, 2], "column2": ["value1", "value2"]}
    )

    # Asserting the result matches the expected DF
    pd.testing.assert_frame_equal(result, expected_output)
    mock_transform_function.assert_called_once_with(data)
    mock_logger.info.assert_called_once_with(f"Processing table: {table_name}")


@patch("src.transformation.transformationutil.logger")
def test_process_table_empty_data(mock_logger):
    # Mock transformation function
    mock_transform_function = Mock()
    mock_transform_function.return_value = pd.DataFrame(
        columns=["column1", "column2"]
    )

    table_name = "test_table"
    data = []

    result = process_table(table_name, mock_transform_function, data)

    expected_output = pd.DataFrame(columns=["column1", "column2"])

    # Asserting the result matches the expected :P
    pd.testing.assert_frame_equal(result, expected_output)
    mock_transform_function.assert_called_once_with(data)
    mock_logger.info.assert_called_once_with(f"Processing table: {table_name}")


@patch("src.transformation.transformationutil.logger")
def test_process_table_invalid_table_name(mock_logger):
    # Mock transformation function
    mock_transform_function = Mock()
    mock_transform_function.return_value = pd.DataFrame(
        {"column1": [1], "column2": ["value1"]}
    )

    table_name = None
    data = [{"column1": 1, "column2": "value1"}]

    result = process_table(table_name, mock_transform_function, data)

    expected_output = pd.DataFrame({"column1": [1], "column2": ["value1"]})

    # Asserting the result matches the expected :O
    pd.testing.assert_frame_equal(result, expected_output)
    mock_transform_function.assert_called_once_with(data)
    mock_logger.info.assert_called_once_with("Processing table: None")
