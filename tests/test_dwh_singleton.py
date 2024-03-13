from helpers import grs, mock_pyodbc_connection
from pyprediktorutilities.dwh import dwh_singleton


def test_dwh_singleton_can_be_created_only_once(monkeypatch):
    driver_index = 0

    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktorutilities.dwh.dwh.pyodbc.connect", mock_pyodbc_connection
    )

    db = dwh_singleton.DwhSingleton(grs(), grs(), grs(), grs(), driver_index)
    db_instance_address = id(db)

    db_2 = dwh_singleton.DwhSingleton(grs(), grs(), grs(), grs(), driver_index)
    db_2_instance_address = id(db_2)

    assert db_instance_address == db_2_instance_address
