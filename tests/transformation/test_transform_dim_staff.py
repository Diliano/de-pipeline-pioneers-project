from src.transformation.transformationutil import transform_dim_staff
import pandas as pd

# Mock
# Testing data
staff_data = [
    {
        "staff_id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "department_id": 101,
        "email_address": "john.doe@example.com",
        "created_at": "2023-01-01",
        "last_updated": "2023-05-01",
    },
    {
        "staff_id": 2,
        "first_name": "Jane",
        "last_name": "Smith",
        "department_id": 102,
        "email_address": "jane.smith@example.com",
        "created_at": "2023-02-01",
        "last_updated": "2023-06-01",
    },
]
department_data = [
    {
        "department_id": 101,
        "department_name": "Engineering",
        "location": "New York",
        "manager": "Alice",
        "created_at": "2023-01-01",
        "last_updated": "2023-05-01",
    },
    {
        "department_id": 102,
        "department_name": "HR",
        "location": "London",
        "manager": "Bob",
        "created_at": "2023-02-01",
        "last_updated": "2023-06-01",
    },
]
# Expected result for the main transformation test
expected_dim_staff = pd.DataFrame([
    {
        "staff_id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "department_name": "Engineering",
        "location": "New York",
        "email_address": "john.doe@example.com",
    },
    {
        "staff_id": 2,
        "first_name": "Jane",
        "last_name": "Smith",
        "department_name": "HR",
        "location": "London",
        "email_address": "jane.smith@example.com",
    },
])

# Test cases for different cases


def test_transform_dim_staff_return_type():
    # Arrange
    expected_staff_data = staff_data
    expected_department_data = department_data

    # Act
    result = transform_dim_staff(expected_staff_data, expected_department_data)
    # Assert
    assert isinstance(
        result, pd.DataFrame), f"Expected Dataframe, got {type(result)}"


def test_transform_dim_staff_correct_transformation():
    # Act
    result = transform_dim_staff(staff_data, department_data)
    # Assert
    pd.testing.assert_frame_equal(result, expected_dim_staff)
    print("test_transform_dim_staff_correct_transformation passed")


def test_transform_dim_staff_empty_inputs():
    # Arrange
    empty_staff_data = pd.DataFrame(columns=[
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    ])
    empty_department_data = pd.DataFrame(columns=[
        "department_id",
        "department_name",
        "location",
        "manager",
        "created_at",
        "last_updated",
    ])
    expected = pd.DataFrame(columns=[
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ])
    # Act
    result = transform_dim_staff(empty_staff_data, empty_department_data)

    # Assert
    pd.testing.assert_frame_equal(result, expected)
    print("test_transform_dim_staff_empty_inputs passed")


def test_transform_dim_staff_missing_columns_handled_gracefully():
    # Arrange
    complete_staff_data = pd.DataFrame([
        {
            "staff_id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "department_id": 101,
            "email_address": "john.doe@example.com",
            "created_at": "2023-01-01",
            "last_updated": "2023-05-01",
        }
    ])
    incomplete_department_data = pd.DataFrame([
        {
            "department_id": 101,
            "department_name": "Engineering",
            "location": "New York"},
    ])
    expected = pd.DataFrame([
        {
            "staff_id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "department_name": "Engineering",
            "location": "New York",
            "email_address": "john.doe@example.com",
        }
    ])

    # Act
    result = transform_dim_staff(
        complete_staff_data, incomplete_department_data)

    # Assert
    pd.testing.assert_frame_equal(result, expected)
    print("test_transform_dim_staff_missing_columns_handled_gracefully passed")


def test_transform_dim_staff_no_matching_departments():
    # Arrange
    unmatched_department_data = pd.DataFrame([
        {
            "department_id": 201,
            "department_name": "Marketing",
            "location": "Tokyo",
            "manager": "Charlie",
            "created_at": "2023-01-01",
            "last_updated": "2023-05-01",
        }
    ])

    expected = pd.DataFrame({
        "staff_id": pd.Series(dtype="int64"),
        "first_name": pd.Series(dtype="object"),
        "last_name": pd.Series(dtype="object"),
        "department_name": pd.Series(dtype="object"),
        "location": pd.Series(dtype="object"),
        "email_address": pd.Series(dtype="object"),
    })
    # Act
    result = transform_dim_staff(staff_data, unmatched_department_data)
    print(result)
    # Assert
    pd.testing.assert_frame_equal(result, expected)
    print("test_transform_dim_staff_no_matching_departments passed")
