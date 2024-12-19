import logging
from typing import Any

import pyodbc

logging.basicConfig(level=logging.INFO)


QUERIES_LIST = {
    "Main plant parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Main plant parameters';",
    "DWH Extract Parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='DWH Extract Parameters';",
    "UA model parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='UA model parameters';",
    "SolarGIS parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='SolarGIS parameters';",
    "Sub facilities": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Sub facilities';",
    "String combiner parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='String combiner';",
    "Transformer parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Transformer parameters';",
    "Module-Mounting stru parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Module-Mounting stru parameters';",
    "Inverter parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Inverter parameters';",
    "String Channel parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='String Channel parameters';",
    "Tracker parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Tracker parameters';",
    "Weather station parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Weather station parameters';",
    "Sub Station": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Sub Station';",
    "Sub Station Transformer": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Sub Station Transformer';",
    "Feeder": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Feeder';",
    "Grid Connection Point": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Grid Connection Point';",
    "PPC Parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='PPC Parameters';",
    "Meter parameters": "exec [dwetl].[GetPlantParametersData] @Facilityname='{}', @DataType='Meter parameters';",
}


class Dwh:
    def __init__(
        self,
        url: str,
        database: str,
        username: str,
        password: str,
        site: str,
        driver_index: int = -1,
    ) -> None:
        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self.site = site

        self.driver = ""
        self.cursor = None
        self.connection = None
        self.__set_driver(driver_index)

        self.connection_string = (
            f"UID={self.username};"
            + f"PWD={self.password};"
            + f"DRIVER={self.driver};"
            + f"SERVER={self.url};"
            + f"DATABASE={self.database};"
            + "TrustServerCertificate=yes;"
        )
        self.connection_attempts = 3

    def __enter__(self):
        self.__connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.__disconnect()

    def get_data_for_tab(self, tab_name: str) -> list[tuple]:
        query = QUERIES_LIST[tab_name].format(self.site)
        result = self.fetch(query=query)

        if not result:
            logging.warning(f"No data fetched for tab: {tab_name!r} for site: {self.site!r}.")
        return result

    def fetch(self, query: str) -> list[Any]:
        self.__connect()
        self.cursor.execute(query)

        results = []
        while True:
            for row in self.cursor.fetchall():
                results.append(tuple(row))

            if not self.cursor.nextset():
                break

        self.__disconnect()  # prevent from leaving open transactions in DWH
        return results

    def __set_driver(self, driver_index: int) -> None:
        if driver_index < 0:
            drivers = self.__get_list_of_available_and_supported_pyodbc_drivers()
            if len(drivers) > 0:
                self.driver = drivers[0]
            return

        if self.__get_number_of_available_pyodbc_drivers() < (driver_index + 1):
            raise ValueError(
                f"Driver index {driver_index} is out of range. Please use "
                + f"the __get_list_of_available_pyodbc_drivers() method "
                + f"to list all available drivers."
            )

        self.driver = self.__get_list_of_supported_pyodbc_drivers()[driver_index]

    def __get_number_of_available_pyodbc_drivers(self) -> int:
        return len(self.__get_list_of_supported_pyodbc_drivers())

    def __get_list_of_supported_pyodbc_drivers(self) -> list[Any]:
        return pyodbc.drivers()

    def __get_list_of_available_and_supported_pyodbc_drivers(self) -> list[Any]:
        available_drivers = []
        for driver in self.__get_list_of_supported_pyodbc_drivers():
            try:
                pyodbc.connect(
                    f"UID={self.username};"
                    + f"PWD={self.password};"
                    + f"DRIVER={driver};"
                    + f"SERVER={self.url};"
                    + f"DATABASE={self.database};",
                    timeout=3,
                )
                available_drivers.append(driver)
            except pyodbc.Error as e:
                pass

        return available_drivers

    def __connect(self) -> None:
        if self.connection:
            return

        attempt = 0
        while attempt < self.connection_attempts:
            try:
                self.connection = pyodbc.connect(self.connection_string)
                self.cursor = self.connection.cursor()
                break

            # Exceptions once thrown there is no point attempting
            except pyodbc.DataError as err:
                logging.error(f"Data Error {err.args[0]}: {err.args[1]}")
                raise
            except pyodbc.IntegrityError as err:
                logging.error(f"Integrity Error {err.args[0]}: {err.args[1]}")
                raise
            except pyodbc.ProgrammingError as err:
                logging.error(f"Programming Error {err.args[0]}: {err.args[1]}")
                logging.warning(
                    f"There seems to be a problem with your code. Please "
                    + f"check your code and try again."
                )
                raise
            except pyodbc.NotSupportedError as err:
                logging.error(f"Not supported {err.args[0]}: {err.args[1]}")
                raise

            # Exceptions when thrown we can continue attempting
            except pyodbc.OperationalError as err:
                logging.error(f"Operational Error {err.args[0]}: {err.args[1]}")
                logging.warning(
                    f"Pyodbc is having issues with the connection. This "
                    + f"could be due to the wrong driver being used. Please "
                    + f"check your driver with "
                    + f"the __get_list_of_available_and_supported_pyodbc_drivers() method "
                    + f"and try again."
                )

                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    raise
            except pyodbc.DatabaseError as err:
                logging.error(f"Database Error {err.args[0]}: {err.args[1]}")

                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    raise
            except pyodbc.Error as err:
                logging.error(f"Generic Error {err.args[0]}: {err.args[1]}")

                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    raise

    def __are_connection_attempts_reached(self, attempt) -> bool:
        if attempt != self.connection_attempts:
            logging.warning("Retrying connection...")
            return False

        logging.error(
            f"Failed to connect to the DataWarehouse after "
            + f"{self.connection_attempts} attempts."
        )
        return True

    def __disconnect(self) -> None:
        if self.connection:
            self.connection.close()

            self.cursor = None
            self.connection = None

    def __commit(self) -> None:
        self.connection.commit()
