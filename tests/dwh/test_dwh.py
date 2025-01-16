import helpers
from pyprediktorutilities.dwh import dwh

from unittest import mock

import pandas as pd
import pyodbc
import pytest
from pandas.testing import assert_frame_equal


class TestDwh:
    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__set_driver")
    def test_init_when_instantiate_db_then_instance_is_created(self, _, monkeypatch):
        driver_index = 0

        with mock.patch("pyprediktorutilities.dwh.dwh.pyodbc.connect"):
            db = dwh.Dwh(
                helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
            )

        assert db is not None

    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__set_driver")
    def test_dwh_connect_when_connection_is_present(self, _, dwh_instance):
        dwh_instance.connection = mock.Mock()
        result = dwh_instance._Dwh__connect()
        assert result is None

    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__set_driver")
    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__connect")
    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__disconnect")
    def test_dwh_use_as_context_manager_when_connection_is_present(
        self, disconnect_mock, connect_mock, set_driver_mock, dwh_instance
    ):
        with dwh_instance:
            pass

        assert not disconnect_mock.called

    def test_init_when_instantiate_dwh_but_driver_index_is_not_passed_then_instance_is_created(
        self,
        monkeypatch,
    ):
        # Mock the connection method to return a mock connection with a mock cursor
        mock_cursor = mock.Mock()
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2"])

        db = dwh.Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs())
        assert db is not None
        assert db.driver == "Driver1"

    """
    fetch
    """

    def test_fetch_when_to_dataframe_is_false_and_no_data_is_returned_then_return_empty_list(
        self,
        monkeypatch,
    ):
        query = "SELECT * FROM mytable"
        driver_index = 2

        expected_result = []

        # Mock the cursor's fetchall methods
        mock_cursor = mock.Mock()
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
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
        actual_result = db.fetch(query)

        mock_cursor.execute.assert_called_once_with(query)
        assert actual_result == expected_result
        assert db.connection is None

    def test_fetch_when_to_dataframe_is_false_and_single_data_set_is_returned_then_return_list_representing_single_data_set(
        self,
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
        mock_cursor = mock.Mock()
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
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
        actual_result = db.fetch(query)

        mock_cursor.execute.assert_called_once_with(query)
        assert actual_result == expected_result
        assert db.connection is None

    def test_fetch_when_to_dataframe_is_false_and_multiple_data_sets_are_returned_then_return_list_of_lists_representing_multiple_data_sets(
        self,
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
        mock_cursor = mock.Mock()
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
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
        actual_result = db.fetch(query)

        mock_cursor.execute.assert_called_once_with(query)
        assert actual_result == expected_result
        assert db.connection is None

    def test_fetch_when_to_dataframe_is_true_and_no_data_is_returned_then_return_empty_dataframe(
        self,
        monkeypatch,
    ):
        query = "SELECT * FROM mytable"
        driver_index = 2

        # Mock the cursor's fetchall methods
        mock_cursor = mock.Mock()
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
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
        actual_result = db.fetch(query, True)

        mock_cursor.execute.assert_called_once_with(query)
        assert actual_result.empty
        assert db.connection is None

    def test_fetch_when_to_dataframe_is_true_and_single_data_set_is_returned_then_return_dataframe(
        self,
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
        mock_cursor = mock.Mock()
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
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
        actual_result = db.fetch(query, True)

        mock_cursor.execute.assert_called_once_with(query)
        assert_frame_equal(
            actual_result.reset_index(drop=True),
            expected_df.reset_index(drop=True),
            check_dtype=False,
        )
        assert db.connection is None

    def test_fetch_when_to_dataframe_is_true_and_multiple_data_sets_are_returned_then_return_list_of_dataframes_representing_multiple_data_sets(
        self,
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
        mock_cursor = mock.Mock()
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
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
        monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
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
        assert db.connection is None

    """
    execute
    """

    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__set_driver")
    def test_execute_when_parameter_passed_then_fetch_results_and_return_data(
        self, _, monkeypatch
    ):
        query = "INSERT INTO mytable VALUES (?, ?)"
        param_one = "John"
        param_two = "Smith"
        driver_index = 0
        expected_result = [{"id": 13}]

        # Mock the cursor and execute
        mock_cursor = mock.Mock()
        mock_execute = mock.Mock()

        # Mock the connection method to return a mock connection with a mock cursor
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor

        monkeypatch.setattr(
            "pyodbc.connect",
            mock.Mock(return_value=mock_connection),
        )

        # Mock the fetch method
        mock_fetch = mock.Mock(return_value=expected_result)
        mock_cursor.execute = mock_execute
        mock_cursor.fetchall = mock_fetch

        db = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )
        actual_result = db.execute(query, param_one, param_two)

        mock_execute.assert_called_once_with(query, param_one, param_two)
        mock_fetch.assert_called_once()
        assert actual_result == expected_result
        assert db.connection is None

    def test_init_when_instantiate_db_but_no_pyodbc_drivers_available_then_throw_exception(
        self, monkeypatch
    ):
        driver_index = 0
        monkeypatch.setattr("pyodbc.drivers", lambda: ["DRIVER1"])
        monkeypatch.setattr(
            dwh.Dwh,
            "_Dwh__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: {"available": [], "supported": []},
        )

        with pytest.raises(ValueError, match="No supported ODBC drivers found."):
            dwh.Dwh(
                helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
            )

    def test_init_with_out_of_range_driver_index(self, monkeypatch):
        driver_index = 1
        monkeypatch.setattr("pyodbc.drivers", lambda: ["DRIVER1"])
        monkeypatch.setattr(
            dwh.Dwh,
            "_Dwh__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: {"available": ["DRIVER1"], "supported": ["DRIVER1"]},
        )

        with pytest.raises(ValueError, match="Driver index 1 is out of range."):
            dwh.Dwh(
                helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
            )

    def test_init_sets_driver_into_connection_string(self, monkeypatch):
        driver_name = "MY_MSSQL_DRIVER"
        monkeypatch.setattr("pyodbc.drivers", lambda: [driver_name])
        monkeypatch.setattr(
            dwh.Dwh,
            "_Dwh__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: {"available": [driver_name], "supported": [driver_name]},
        )

        dwh_instance = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs()
        )

        assert driver_name in dwh_instance.connection_string

    @mock.patch("pyodbc.connect")
    def test_context_manager_enter(self, _, dwh_instance):
        assert dwh_instance.__enter__() == dwh_instance

    @mock.patch("pyodbc.connect")
    def test_exit_without_connection(self, dwh_instance):
        dwh_instance.connection = None
        dwh_instance.__exit__(None, None, None)

    @mock.patch("pyodbc.connect")
    def test_exit_with_open_connection_and_cleanup(self, _, dwh_instance):
        mock_connection = mock.Mock()
        dwh_instance.connection = mock_connection
        dwh_instance.__exit__(None, None, None)

        mock_connection.close.assert_called_once()
        assert (
            dwh_instance.connection is None
        ), "Connection should be set to None after exit"

    @pytest.mark.parametrize("to_dataframe", [False, True])
    def test_fetch_multiple_datasets(
        self, mock_pyodbc_connect, dwh_instance, to_dataframe
    ):
        data1 = [("value1", 1), ("value2", 2)]
        data2 = [("value3", 3), ("value4", 4)]
        mock_pyodbc_connect.fetchall.side_effect = [data1, data2]
        mock_pyodbc_connect.nextset.side_effect = [True, False]
        mock_pyodbc_connect.description = [
            ("column1", None),
            ("column2", None),
        ]

        result = dwh_instance.fetch("SELECT * FROM test_table", to_dataframe)

        if to_dataframe:
            expected1 = pd.DataFrame(data1, columns=["column1", "column2"])
            expected2 = pd.DataFrame(data2, columns=["column1", "column2"])
            assert len(result) == 2
            assert_frame_equal(result[0], expected1)
            assert_frame_equal(result[1], expected2)
        else:
            expected = [
                [
                    {"column1": "value1", "column2": 1},
                    {"column1": "value2", "column2": 2},
                ],
                [
                    {"column1": "value3", "column2": 3},
                    {"column1": "value4", "column2": 4},
                ],
            ]
            assert result == expected

    def test_execute_with_fetch_error(self, dwh_instance, mock_pyodbc_connect, caplog):
        mock_pyodbc_connect.fetchall.side_effect = Exception("Fetch error")

        result = dwh_instance.execute("SELECT * FROM test_table")

        assert result == []
        assert "Failed to execute query: Fetch error" in caplog.text

    def test_set_driver_with_valid_index(self, monkeypatch):
        available_drivers = {
            "available": ["DRIVER1", "DRIVER2"],
            "supported": ["DRIVER1", "DRIVER2"],
        }
        monkeypatch.setattr(
            dwh.Dwh,
            "_Dwh__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: available_drivers,
        )

        driver_index = 1
        dwh_instance = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs(), driver_index
        )

        assert dwh_instance.driver == "DRIVER2"

    def test_get_list_of_supported_pyodbc_drivers_error(
        self, dwh_instance, monkeypatch, caplog
    ):
        monkeypatch.setattr(
            pyodbc, "drivers", mock.Mock(side_effect=pyodbc.Error("Test error"))
        )

        with pytest.raises(pyodbc.Error):
            dwh.Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs())

    @mock.patch("pyodbc.connect")
    @mock.patch(
        "pyprediktorutilities.dwh.dwh.Dwh._Dwh__get_list_of_supported_pyodbc_drivers"
    )
    def test_get_sets_available_driver(self, mock_get_supported_drivers, mock_connect):
        mock_get_supported_drivers.return_value = ["Driver1", "Driver2"]
        mock_connect.side_effect = [pyodbc.Error, None]

        dwh_instance = dwh.Dwh(
            helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs()
        )

        assert mock_connect.call_count == 2
        assert dwh_instance.driver == "Driver2"

    @mock.patch("pyodbc.drivers")
    @mock.patch("pyprediktorutilities.dwh.dwh.Dwh._Dwh__set_driver")
    def test_get_supported_but_not_available_drivers_raises_error(
        self, mock_drivers, mock_connect
    ):
        mock_drivers.return_value = ["Driver1", "Driver2", "Driver3"]
        mock_connect.side_effect = [pyodbc.Error, pyodbc.Error, pyodbc.Error]
        dwh.Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs())

    def test_connect_success(self, dwh_instance, monkeypatch):
        mock_connection = mock.Mock()
        monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)

        dwh_instance.connection = None
        dwh_instance._Dwh__connect()

        assert dwh_instance.connection is mock_connection
        assert dwh_instance.cursor is not None

    def test_connect_raises_data_error(self, dwh_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.DataError("Data error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        dwh_instance.connection = None
        with pytest.raises(pyodbc.DataError):
            dwh_instance._Dwh__connect()

    def test_connect_raises_integrity_error(self, dwh_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.IntegrityError("Integrity error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        dwh_instance.connection = None
        with pytest.raises(pyodbc.IntegrityError):
            dwh_instance._Dwh__connect()

    def test_connect_raises_programming_error(self, dwh_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.ProgrammingError("Programming error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        dwh_instance.connection = None
        with pytest.raises(pyodbc.ProgrammingError):
            dwh_instance._Dwh__connect()

    def test_connect_raises_not_supported_error(self, dwh_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.NotSupportedError("Not supported error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        dwh_instance.connection = None
        with pytest.raises(pyodbc.NotSupportedError):
            dwh_instance._Dwh__connect()

    def test_connect_retries_on_operational_error(self, dwh_instance, monkeypatch):
        attempt_count = 0

        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.OperationalError("Operational error")
            return mock.Mock()

        dwh_instance.connection_attempts = 3
        dwh_instance.connection = None

        with mock.patch("pyodbc.connect", side_effect=mock_connect):
            dwh_instance._Dwh__connect()

        assert attempt_count == 3, "Should attempt three connections before succeeding"
        assert (
            dwh_instance.connection is not None
        ), "Connection should be established after retries"

    def test_connect_raises_after_max_attempts_on_operational_error(
        self, dwh_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.OperationalError("Operational error")

        dwh_instance.connection = None
        dwh_instance.connection_attempts = 3
        with mock.patch("pyodbc.connect", side_effect=mock_connect):
            with pytest.raises(pyodbc.Error, match="Failed to connect to the database"):
                dwh_instance._Dwh__connect()

    def test_connect_retries_on_database_error(self, dwh_instance, monkeypatch):
        attempt_count = 0

        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.DatabaseError("Database error")
            return mock.Mock()

        dwh_instance.connection_attempts = 3
        dwh_instance.connection = None

        with mock.patch("pyodbc.connect", side_effect=mock_connect):
            dwh_instance._Dwh__connect()

        assert attempt_count == 3, "Should attempt three connections before succeeding"

    def test_connect_raises_after_max_attempts_on_database_error(
        self, dwh_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.DatabaseError("Database error")

        dwh_instance.connection = None
        dwh_instance.connection_attempts = 3
        with mock.patch("pyodbc.connect", side_effect=mock_connect):
            with pytest.raises(pyodbc.Error, match="Failed to connect to the database"):
                dwh_instance._Dwh__connect()

    def test_connect_retries_on_generic_error(self, dwh_instance, monkeypatch):
        attempt_count = 0

        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.Error("Generic error")
            return mock.Mock()

        dwh_instance.connection_attempts = 3
        dwh_instance.connection = None

        with mock.patch("pyodbc.connect", side_effect=mock_connect):
            dwh_instance._Dwh__connect()

        assert attempt_count == 3, "Should attempt three connections before succeeding"

    def test_connect_raises_after_max_attempts_on_generic_error(
        self, dwh_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.Error("Generic error")

        dwh_instance.connection = None
        dwh_instance.connection_attempts = 3
        with mock.patch("pyodbc.connect", side_effect=mock_connect):
            with pytest.raises(pyodbc.Error, match="Failed to connect to the database"):
                dwh_instance._Dwh__connect()

    def test_connect_raises_error_when_connection_is_none(
        self, dwh_instance, monkeypatch
    ):

        def mock_connect(*args, **kwargs):
            return None

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        dwh_instance.connection_attempts = 3
        dwh_instance.connection = None

        with pytest.raises(pyodbc.Error, match="Failed to connect to the database"):
            dwh_instance._Dwh__connect()

    def test_are_connection_attempts_reached(self, dwh_instance, caplog):
        assert not dwh_instance._Dwh__are_connection_attempts_reached(1)
        assert "Retrying connection..." in caplog.text

        assert dwh_instance._Dwh__are_connection_attempts_reached(3)
        assert "Failed to connect to the DataWarehouse after 3 attempts." in caplog.text

    def test_disconnect(self, dwh_instance):
        mock_connection = mock.Mock()
        dwh_instance.connection = mock_connection
        dwh_instance.cursor = mock.Mock()

        dwh_instance._Dwh__disconnect()

        assert mock_connection.close.called
        assert dwh_instance.connection is None
        assert dwh_instance.cursor is None

    def test_disconnect_without_connection(self, dwh_instance):
        dwh_instance.connection = None
        dwh_instance.cursor = None

        dwh_instance._Dwh__disconnect()

    def test_commit(self, dwh_instance):
        mock_connection = mock.Mock()
        dwh_instance.connection = mock_connection
        dwh_instance._Dwh__commit()
        mock_connection.commit.assert_called_once()
