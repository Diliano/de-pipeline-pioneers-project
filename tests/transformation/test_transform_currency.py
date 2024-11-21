from src.transformation import transform_dim_currency
import pandas as pd
from datetime import datetime
import pandas.testing as test


def test_empty_currency_data():
    # Arrange
    currency_data = []

    # Act
    result = transform_dim_currency(currency_data)

    # Assert
    assert result is None


def test_transform_currency_success():
    # Arrange
    currency_data = [
        [
            1,
            "GBP",
            datetime(2022, 11, 3, 14, 20, 49),
            datetime(2022, 11, 3, 14, 20, 49),
        ],
        [
            2,
            "USD",
            datetime(2022, 11, 3, 14, 20, 49),
            datetime(2022, 11, 3, 14, 20, 49),
        ],
    ]

    expected_output = pd.DataFrame(
        {
            "currency_id": [1, 2],
            "currency_code": ["GBP", "USD"],
            "currency_name": ["British Pound", "US Dollar"],
        }
    )

    # Act
    result = transform_dim_currency(currency_data)

    # Assert
    test.assert_frame_equal(result, expected_output)


def test_for_duplicate_currencys():
    # Arrange
    currency_data = [
        [
            1,
            "GBP",
            datetime(2022, 11, 3, 14, 20, 49),
            datetime(2022, 11, 3, 14, 20, 49),
        ],
        [
            1,
            "GBP",
            datetime(2022, 11, 3, 14, 20, 49),
            datetime(2022, 11, 3, 14, 20, 49),
        ],
    ]

    expected_output = pd.DataFrame(
        {
            "currency_id": [1],
            "currency_code": ["GBP"],
            "currency_name": ["British Pound"],
        }
    )

    # Act
    result = transform_dim_currency(currency_data)

    # Assert
    test.assert_frame_equal(result, expected_output)


def test_for_unknown_currency():
    # Arrange
    currency_data = [
        [
            1,
            "BTC",
            datetime(2022, 11, 3, 14, 20, 49),
            datetime(2022, 11, 3, 14, 20, 49),
        ],
        [
            2,
            "LTC",
            datetime(2022, 11, 3, 14, 20, 49),
            datetime(2022, 11, 3, 14, 20, 49),
        ],
    ]

    expected_output = pd.DataFrame(
        {
            "currency_id": [1, 2],
            "currency_code": ["BTC", "LTC"],
            "currency_name": ["Unknown Currency", "Unknown Currency"],
        }
    )

    # Act
    result = transform_dim_currency(currency_data)

    # Assert
    test.assert_frame_equal(result, expected_output)
