from unittest import mock

import pytest

import helpers
from pyprediktorutilities.dwh import dwh


@pytest.fixture()
@mock.patch(
    "pyprediktorutilities.dwh.dwh.Dwh._Dwh__get_list_of_available_and_supported_pyodbc_drivers",
    return_value={"available": ["Driver1"], "supported": ["Driver1"]},
)
def dwh_instance(_, monkeypatch):
    dwh_instance = dwh.Dwh(helpers.grs(), helpers.grs(), helpers.grs(), helpers.grs())
    return dwh_instance


@pytest.fixture
def mock_pyodbc_connect(monkeypatch):
    mock_connection = mock.Mock()
    mock_cursor = mock.Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    return mock_cursor
