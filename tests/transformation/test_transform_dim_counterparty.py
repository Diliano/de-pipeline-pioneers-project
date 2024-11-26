import pandas as pd
from src.transformation.transformationutil import transform_dim_counterparty
# pip install pytest-mock


def test_empty_data():
    """
    Test with empty data for both counterparty and address.
    """
    counterparty_data = []
    address_data = []

    result = transform_dim_counterparty(counterparty_data, address_data)
    assert result.empty


def test_transform_dim_counterparty_with_patch():
    """
    Test full functionality.
    """
    # Mock input data for counterparty and address
    counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "legal_address_id": 101,
            "commercial_contact": "A A",
            "delivery_contact": "B B",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        },
        {
            "counterparty_id": 2,
            "counterparty_legal_name": "Fake Company B",
            "legal_address_id": 102,
            "commercial_contact": "C C",
            "delivery_contact": "D D",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        },
    ]
    address_data = [
        {
            "address_id": 101,
            "address_line_1": "1 First St",
            "address_line_2": "Floor 1",
            "district": "12",
            "city": "Hogwarts",
            "postal_code": "12345",
            "country": "Paradis",
            "phone": "123-456-7890",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        },
        {
            "address_id": 102,
            "address_line_1": "456 Elm St",
            "address_line_2": None,
            "district": "13",
            "city": "Gotham",
            "postal_code": "67890",
            "country": "Narnia",
            "phone": "987-654-3210",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        },
    ]
    # Call the function with the mocked data
    result = transform_dim_counterparty(counterparty_data, address_data)
    # Define the expected output
    expected_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "counterparty_legal_address_line_1": "1 First St",
            "counterparty_legal_address_line_2": "Floor 1",
            "counterparty_legal_district": "12",
            "counterparty_legal_city": "Hogwarts",
            "counterparty_legal_postal_code": "12345",
            "counterparty_legal_country": "Paradis",
            "counterparty_legal_phone_number": "123-456-7890",
        },
        {
            "counterparty_id": 2,
            "counterparty_legal_name": "Fake Company B",
            "counterparty_legal_address_line_1": "456 Elm St",
            "counterparty_legal_address_line_2": None,
            "counterparty_legal_district": "13",
            "counterparty_legal_city": "Gotham",
            "counterparty_legal_postal_code": "67890",
            "counterparty_legal_country": "Narnia",
            "counterparty_legal_phone_number": "987-654-3210",
        },
    ]

    expected_df = pd.DataFrame(expected_data)

    # Assert the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result, expected_df)


def test_missing_address_for_counterparty():
    """
    Test where counterparty refers to a legal_address_id
    not present in address data.
    """
    counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "legal_address_id": 101,
            "commercial_contact": "A A",
            "delivery_contact": "B B",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        }
    ]
    address_data = []

    result = transform_dim_counterparty(counterparty_data, address_data)

    # Result should be an empty DataFrame because there's no address match
    assert result.empty


def test_extra_columns_in_input():
    """
    Test with additional unexpected columns in the input data.
    """
    counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "legal_address_id": 101,
            "commercial_contact": "A A",
            "delivery_contact": "B B",
            "extra_column": "why you here",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        }
    ]
    address_data = [
        {
            "address_id": 101,
            "address_line_1": "1 First St",
            "address_line_2": "Floor 1",
            "district": "12",
            "city": "Hogwarts",
            "postal_code": "12345",
            "country": "Paradis",
            "phone": "123-456-7890",
            "extra_column": "why you here??",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        }
    ]

    result = transform_dim_counterparty(counterparty_data, address_data)

    # Ensure the output ignores unexpected columns and still
    # processes correctly
    expected_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "counterparty_legal_address_line_1": "1 First St",
            "counterparty_legal_address_line_2": "Floor 1",
            "counterparty_legal_district": "12",
            "counterparty_legal_city": "Hogwarts",
            "counterparty_legal_postal_code": "12345",
            "counterparty_legal_country": "Paradis",
            "counterparty_legal_phone_number": "123-456-7890",
        }
    ]
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)


def test_null_values_in_data():
    """
    Test with null values in non-critical columns.
    """
    counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "legal_address_id": 101,
            "commercial_contact": None,
            "delivery_contact": None,
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        }
    ]
    address_data = [
        {
            "address_id": 101,
            "address_line_1": "1 First St",
            "address_line_2": None,  # Nullable field
            "district": None,  # Nullable field
            "city": "Hogwarts",
            "postal_code": "12345",
            "country": "Paradis",
            "phone": "123-456-7890",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        }
    ]

    result = transform_dim_counterparty(counterparty_data, address_data)

    expected_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "counterparty_legal_address_line_1": "1 First St",
            "counterparty_legal_address_line_2": None,
            "counterparty_legal_district": None,
            "counterparty_legal_city": "Hogwarts",
            "counterparty_legal_postal_code": "12345",
            "counterparty_legal_country": "Paradis",
            "counterparty_legal_phone_number": "123-456-7890",
        }
    ]
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)


def test_partial_data_merge():
    """
    Test where not all counterparty data matches addresses.
    """
    counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "legal_address_id": 101,
            "commercial_contact": "A A",
            "delivery_contact": "B B",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        },
        {
            "counterparty_id": 2,
            "counterparty_legal_name": "Fake Company B",
            "legal_address_id": 102,
            "commercial_contact": "C C",
            "delivery_contact": "D D",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        },
    ]

    address_data = [
        {
            "address_id": 101,
            "address_line_1": "1 First St",
            "address_line_2": "Floor 1",
            "district": "12",
            "city": "Hogwarts",
            "postal_code": "12345",
            "country": "Paradis",
            "phone": "123-456-7890",
            "created_at": "2024-01-01T00:00:00",
            "last_updated": "2024-01-01T00:00:00",
        }
    ]

    result = transform_dim_counterparty(counterparty_data, address_data)

    expected_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fake Company A",
            "counterparty_legal_address_line_1": "1 First St",
            "counterparty_legal_address_line_2": "Floor 1",
            "counterparty_legal_district": "12",
            "counterparty_legal_city": "Hogwarts",
            "counterparty_legal_postal_code": "12345",
            "counterparty_legal_country": "Paradis",
            "counterparty_legal_phone_number": "123-456-7890",
        }
    ]
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)
