import logging
from unittest.mock import Mock

import pandas as pd
import pyodbc
import pytest
from pandas.testing import assert_frame_equal

import helpers
from pyprediktorutilities.dwh.dwh import Dwh


def test_init_when_instantiate_db_then_instance_is_created(monkeypatch):
    driver_index = 0

    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktorutilities.dwh.dwh.pyodbc.connect", helpers.mock_pyodbc_connection
    )

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    assert db is not None


def test_init_when_instantiate_db_but_no_pyodbc_drivers_available_then_throw_exception(
    monkeypatch,
):
    driver_index = 0

    # Mock the absence of ODBC drivers
    monkeypatch.setattr("pyprediktorutilities.dwh.dwh.pyodbc.drivers", lambda: [])

    with pytest.raises(ValueError) as excinfo:
        Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    assert "Driver index 0 is out of range." in str(excinfo.value)


def test_init_when_instantiate_db_but_pyodbc_throws_error_with_tolerance_to_attempts_then_throw_exception(
    monkeypatch,
):
    driver_index = 0

    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktorutilities.dwh.dwh.pyodbc.connect",
        helpers.mock_pyodbc_connection_throws_error_not_tolerant_to_attempts,
    )

    with pytest.raises(pyodbc.DataError):
        Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)


def test_init_when_instantiate_db_but_pyodbc_throws_error_tolerant_to_attempts_then_retry_connecting_and_throw_exception(
    caplog, monkeypatch
):
    driver_index = 0

    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktorutilities.dwh.dwh.pyodbc.connect",
        helpers.mock_pyodbc_connection_throws_error_tolerant_to_attempts,
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(pyodbc.DatabaseError):
            Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)

    assert any(
        "Failed to connect to the DataWarehouse after 3 attempts." in message
        for message in caplog.messages
    )


def test_init_when_instantiate_dwh_but_driver_index_is_not_passed_then_instance_is_created(
    monkeypatch,
):
    # Mock the connection method to return a mock connection with a mock cursor
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs())
    assert db is not None
    assert db.driver == "Driver1"


"""
fetch
"""


def test_fetch_when_init_db_connection_is_successfull_but_fails_when_calling_fetch_then_throw_exception(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 0

    # Mock the cursor
    mock_cursor = Mock()

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection_success = Mock()
    mock_connection_success.cursor.return_value = mock_cursor

    mock_connection_fail = Mock()
    mock_connection_fail.cursor.side_effect = pyodbc.DataError(
        "Error code", "Database data error"
    )

    monkeypatch.setattr(
        "pyodbc.connect",
        Mock(side_effect=[mock_connection_success, mock_connection_fail]),
    )

    with pytest.raises(pyodbc.DataError):
        db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
        db.connection = False
        db.fetch(query)


def test_fetch_when_to_dataframe_is_false_and_no_data_is_returned_then_return_empty_list(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 2

    expected_result = []

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.nextset.return_value = False
    mock_cursor.description = [
        ("plantname", None),
        ("resource_id", None),
        ("api_key", None),
        ("ExtForecastTypeKey", None),
        ("hours", None),
        ("output_parameters", None),
        ("period", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.fetch(query)

    mock_cursor.execute.assert_called_once_with(query)
    assert actual_result == expected_result


def test_fetch_when_to_dataframe_is_false_and_single_data_set_is_returned_then_return_list_representing_single_data_set(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 2
    data_returned_by_db = [
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            14,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "KL-MN",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
    ]

    expected_result = [
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 14,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "KL-MN",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
    ]

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = data_returned_by_db
    mock_cursor.nextset.return_value = False
    mock_cursor.description = [
        ("plantname", None),
        ("resource_id", None),
        ("api_key", None),
        ("ExtForecastTypeKey", None),
        ("hours", None),
        ("output_parameters", None),
        ("period", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.fetch(query)

    mock_cursor.execute.assert_called_once_with(query)
    assert actual_result == expected_result


def test_fetch_when_to_dataframe_is_false_and_multiple_data_sets_are_returned_then_return_list_of_lists_representing_multiple_data_sets(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 2
    data_returned_by_db_set_one = [
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            14,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "KL-MN",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
    ]
    data_returned_by_db_set_two = [
        (
            "ALPHA",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "BETA",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            14,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
    ]

    expected_result = [
        [
            {
                "plantname": "XY-ZK",
                "resource_id": "1234-abcd-efgh-5678",
                "api_key": "SOME_KEY",
                "ExtForecastTypeKey": 13,
                "hours": 168,
                "output_parameters": "pv_power_advanced",
                "period": "PT15M",
            },
            {
                "plantname": "XY-ZK",
                "resource_id": "1234-abcd-efgh-5678",
                "api_key": "SOME_KEY",
                "ExtForecastTypeKey": 14,
                "hours": 168,
                "output_parameters": "pv_power_advanced",
                "period": "PT15M",
            },
            {
                "plantname": "KL-MN",
                "resource_id": "1234-abcd-efgh-5678",
                "api_key": "SOME_KEY",
                "ExtForecastTypeKey": 13,
                "hours": 168,
                "output_parameters": "pv_power_advanced",
                "period": "PT15M",
            },
        ],
        [
            {
                "plantname": "ALPHA",
                "resource_id": "1234-abcd-efgh-5678",
                "api_key": "SOME_KEY",
                "ExtForecastTypeKey": 13,
                "hours": 168,
                "output_parameters": "pv_power_advanced",
                "period": "PT15M",
            },
            {
                "plantname": "BETA",
                "resource_id": "1234-abcd-efgh-5678",
                "api_key": "SOME_KEY",
                "ExtForecastTypeKey": 14,
                "hours": 168,
                "output_parameters": "pv_power_advanced",
                "period": "PT15M",
            },
        ],
    ]

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [
        data_returned_by_db_set_one,
        data_returned_by_db_set_two,
    ]
    mock_cursor.nextset.side_effect = [True, False]
    mock_cursor.description = [
        ("plantname", None),
        ("resource_id", None),
        ("api_key", None),
        ("ExtForecastTypeKey", None),
        ("hours", None),
        ("output_parameters", None),
        ("period", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.fetch(query)

    mock_cursor.execute.assert_called_once_with(query)
    assert actual_result == expected_result


def test_fetch_when_to_dataframe_is_true_and_no_data_is_returned_then_return_empty_dataframe(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 2

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.nextset.return_value = False
    mock_cursor.description = [
        ("plantname", None),
        ("resource_id", None),
        ("api_key", None),
        ("ExtForecastTypeKey", None),
        ("hours", None),
        ("output_parameters", None),
        ("period", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.fetch(query, True)

    mock_cursor.execute.assert_called_once_with(query)
    assert actual_result.empty


def test_fetch_when_to_dataframe_is_true_and_single_data_set_is_returned_then_return_dataframe(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 2
    data_returned_by_db = [
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            14,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "KL-MN",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
    ]

    expected_result = [
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 14,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "KL-MN",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
    ]
    expected_df = pd.DataFrame(expected_result)

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = data_returned_by_db
    mock_cursor.nextset.return_value = False
    mock_cursor.description = [
        ("plantname", None),
        ("resource_id", None),
        ("api_key", None),
        ("ExtForecastTypeKey", None),
        ("hours", None),
        ("output_parameters", None),
        ("period", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.fetch(query, True)

    mock_cursor.execute.assert_called_once_with(query)
    assert_frame_equal(
        actual_result.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False,
    )


def test_fetch_when_to_dataframe_is_true_and_multiple_data_sets_are_returned_then_return_list_of_dataframes_representing_multiple_data_sets(
    monkeypatch,
):
    query = "SELECT * FROM mytable"
    driver_index = 2
    data_returned_by_db_set_one = [
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "XY-ZK",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            14,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "KL-MN",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
    ]
    data_returned_by_db_set_two = [
        (
            "ALPHA",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            13,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
        (
            "BETA",
            "1234-abcd-efgh-5678",
            "SOME_KEY",
            14,
            168,
            "pv_power_advanced",
            "PT15M",
        ),
    ]

    expected_result_set_one = [
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 14,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "KL-MN",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
    ]
    expected_result_set_two = [
        {
            "plantname": "ALPHA",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "BETA",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 14,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
    ]
    expected_df_set_one = pd.DataFrame(expected_result_set_one)
    expected_df_set_two = pd.DataFrame(expected_result_set_two)

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [
        data_returned_by_db_set_one,
        data_returned_by_db_set_two,
    ]
    mock_cursor.nextset.side_effect = [True, False]
    mock_cursor.description = [
        ("plantname", None),
        ("resource_id", None),
        ("api_key", None),
        ("ExtForecastTypeKey", None),
        ("hours", None),
        ("output_parameters", None),
        ("period", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.fetch(query, True)

    mock_cursor.execute.assert_called_once_with(query)
    assert_frame_equal(
        actual_result[0].reset_index(drop=True),
        expected_df_set_one,
        check_dtype=False,
    )
    assert_frame_equal(
        actual_result[1].reset_index(drop=True),
        expected_df_set_two,
        check_dtype=False,
    )


"""
execute
"""


def test_execute_when_init_db_connection_is_successfull_but_fails_when_calling_execute_then_throw_exception(
    monkeypatch,
):
    query = "INSERT INTO mytable VALUES (1, 'test')"
    driver_index = 0

    # Mock the cursor
    mock_cursor = Mock()

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection_success = Mock()
    mock_connection_success.cursor.return_value = mock_cursor

    mock_connection_fail = Mock()
    mock_connection_fail.cursor.side_effect = pyodbc.DataError(
        "Error code", "Database data error"
    )

    monkeypatch.setattr(
        "pyodbc.connect",
        Mock(side_effect=[mock_connection_success, mock_connection_fail]),
    )

    with pytest.raises(pyodbc.DataError):
        db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
        db.connection = False
        db.execute(query)


def test_execute_when_parameter_passed_then_fetch_results_and_return_data(monkeypatch):
    query = "INSERT INTO mytable VALUES (?, ?)"
    param_one = "John"
    param_two = "Smith"
    driver_index = 0
    expected_result = [{"id": 13}]

    # Mock the cursor and execute
    mock_cursor = Mock()
    mock_execute = Mock()

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor

    monkeypatch.setattr(
        "pyodbc.connect",
        Mock(return_value=mock_connection),
    )

    # Mock the fetch method
    mock_fetch = Mock(return_value=expected_result)
    mock_cursor.execute = mock_execute
    mock_cursor.fetchall = mock_fetch

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.execute(query, param_one, param_two)

    mock_execute.assert_called_once_with(query, param_one, param_two)
    mock_fetch.assert_called_once()
    assert actual_result == expected_result


def test_execute_when_fetchall_throws_error_then_return_empty_list(monkeypatch):
    query = "INSERT INTO mytable VALUES (?, ?)"
    param_one = "John"
    param_two = "Smith"
    driver_index = 0

    # Mock the cursor and execute
    mock_cursor = Mock()
    mock_execute = Mock()
    mock_fetchall = Mock(side_effect=Exception("Error occurred"))

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor

    monkeypatch.setattr(
        "pyodbc.connect",
        Mock(return_value=mock_connection),
    )

    # Mock the fetchall method
    mock_cursor.execute = mock_execute
    mock_cursor.fetchall = mock_fetchall

    db = Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index)
    actual_result = db.execute(query, param_one, param_two)

    mock_execute.assert_called_once_with(query, param_one, param_two)
    mock_fetchall.assert_called_once()
    assert actual_result == []
