import random
import string

import pyodbc


class mock_pyodbc_connection:
    def __init__(self, connection_string):
        pass

    def cursor(self):
        return


def mock_pyodbc_connection_throws_error_not_tolerant_to_attempts(connection_string):
    raise pyodbc.DataError("Error code", "Error message")


def mock_pyodbc_connection_throws_error_tolerant_to_attempts(connection_string):
    raise pyodbc.DatabaseError("Error code", "Error message")


def grs():
    """Generate a random string."""
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
