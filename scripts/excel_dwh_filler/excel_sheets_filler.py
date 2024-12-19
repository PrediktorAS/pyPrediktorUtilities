import abc
import argparse
import copy
import datetime
import logging
from itertools import combinations_with_replacement
from string import ascii_uppercase
from typing import Any, List, Iterator

import openpyxl
import pyodbc
from openpyxl import workbook
from openpyxl.worksheet import worksheet

logging.basicConfig(level=logging.INFO)


class BaseExcelTabProcessor(abc.ABC):
    DATE_FORMAT = "%Y-%m-%d"
    DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, data: list[tuple], tab: worksheet.Worksheet):
        self.data = data
        self.tab = tab

    @abc.abstractmethod
    def run_tab_processing(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def clear_data_from_tab(self) -> None:
        raise NotImplementedError

    def _format_not_convertible_to_string_values(self, value):
        match value:
            case bool():
                value = int(value)
            case float():
                if value.is_integer():
                    value = int(value)
                else:
                    value = as_text(value)
                    value = self._change_separator_from_dot_to_comma(value)
            case datetime.datetime():
                value = value.strftime(self.DATE_TIME_FORMAT)
            case datetime.date():
                value = value.strftime(self.DATE_FORMAT)
        return value

    def _change_separator_from_dot_to_comma(self, float_as_str: str) -> str:
        float_with_comma_separator = as_text(float_as_str).replace(".", ",")
        return float_with_comma_separator


class SingleColumnExcelTabProcessor(BaseExcelTabProcessor):
    VALUE_COLUMN = "B"
    FIRST_ROW_WITH_VALUE_TO_FILL = 1

    def __init__(self, data: list[tuple], tab: worksheet.Worksheet):
        super().__init__(data, tab)
        self.data_tuple = data[0]

    def run_tab_processing(self) -> None:
        self._fill_excel_column_with_values()

    def _fill_excel_column_with_values(self) -> None:
        excel_row_index = copy.copy(self.FIRST_ROW_WITH_VALUE_TO_FILL)

        for row_datum in self.data_tuple:
            transformed_row_datum = self._format_not_convertible_to_string_values(
                row_datum
            )
            formatted_row_datum = as_text(transformed_row_datum)

            cell_location = f"{self.VALUE_COLUMN}{excel_row_index}"
            self.__place_value_at_location(cell_location, formatted_row_datum)

            excel_row_index += 1

    def __place_value_at_location(self, cell_location: str, formatted_row_datum: str):
        self.tab[cell_location] = as_text(formatted_row_datum)

    def clear_data_from_tab(self) -> None:
        logging.warning(
            "Clearing data for the single-column values tabs not implemented. "
            "If you can see this, the implementation should be added!"
        )


class MainPlantParametersExcelTabProcessor(SingleColumnExcelTabProcessor):
    FIRST_ROW_WITH_VALUE_TO_FILL = 4

    TARGET_DWH_OUTPUT_LENGTH = 135
    INDEX_PRIOR_TO_MISSING_GRID_STATION_ID = 14

    def __init__(self, data: list[tuple], tab: worksheet.Worksheet):
        super().__init__(data, tab)
        self.is_missing_grid_station_id = False

    def _fill_excel_column_with_values(self) -> None:
        self.is_missing_grid_station_id = (
            len(self.data_tuple) == self.TARGET_DWH_OUTPUT_LENGTH
        )
        excel_row_index = copy.copy(self.FIRST_ROW_WITH_VALUE_TO_FILL)

        for row_datum in self.data_tuple:
            if isinstance(row_datum, bool):
                row_datum = int(row_datum)
            elif isinstance(row_datum, float):
                if row_datum.is_integer():
                    row_datum = int(row_datum)
                else:
                    row_datum = as_text(row_datum)
                    row_datum = self._change_separator_from_dot_to_comma(row_datum)
            elif isinstance(row_datum, datetime.date):
                row_datum = row_datum.strftime(self.DATE_FORMAT)
            cell_location = f"{self.VALUE_COLUMN}{excel_row_index}"

            self.tab[cell_location] = as_text(row_datum)

            if self.is_missing_grid_station_id:
                if excel_row_index == self.INDEX_PRIOR_TO_MISSING_GRID_STATION_ID:
                    excel_row_index += 1
                    self.tab[f"{self.VALUE_COLUMN}{excel_row_index}"] = ""

            excel_row_index += 1


class DWHExtractParametersExcelTabProcessor(SingleColumnExcelTabProcessor):
    FIRST_ROW_WITH_VALUE_TO_FILL = 3


class UAModelParametersExcelTabProcessor(SingleColumnExcelTabProcessor):
    FIRST_ROW_WITH_VALUE_TO_FILL = 3


class SolarGISParametersExcelTabProcessor(SingleColumnExcelTabProcessor):
    FIRST_ROW_WITH_VALUE_TO_FILL = 5


class MultipleRowsExcelTabProcessor(BaseExcelTabProcessor):
    FIRST_ROW_TO_FILL = 3

    def run_tab_processing(self) -> None:
        self._fill_excel_rows_with_values()

    def _fill_excel_rows_with_values(self) -> None:
        excel_row_index = copy.copy(self.FIRST_ROW_TO_FILL)

        for row_data in self.data:
            excel_column_names_iterator = get_excel_column_names_iterator()
            for cell_datum in row_data:
                column_index = next(excel_column_names_iterator)
                cell_location = f"{column_index}{excel_row_index}"

                transformed_cell_datum = self._format_not_convertible_to_string_values(
                    cell_datum
                )
                formatted_cell_datum = as_text(transformed_cell_datum)
                self.tab[cell_location] = as_text(formatted_cell_datum)
            excel_row_index += 1

    def clear_data_from_tab(self) -> None:
        # clear the data, but keep the formatting:
        for row in self.tab.iter_rows(min_row=self.FIRST_ROW_TO_FILL):
            for cell in row:
                cell.value = None


class DwhExcelSheetsFiller:
    TEMPLATE_NAME = "template.xlsx"
    TAB_NAME_TO_PROCESSOR_MAPPING = {
        "Main plant parameters": MainPlantParametersExcelTabProcessor,
        "DWH Extract Parameters": DWHExtractParametersExcelTabProcessor,
        "UA model parameters": UAModelParametersExcelTabProcessor,
        "SolarGIS parameters": SolarGISParametersExcelTabProcessor,
        "Sub facilities": MultipleRowsExcelTabProcessor,
        "String combiner parameters": MultipleRowsExcelTabProcessor,
        "Transformer parameters": MultipleRowsExcelTabProcessor,
        "Module-Mounting stru parameters": MultipleRowsExcelTabProcessor,
        "Inverter parameters": MultipleRowsExcelTabProcessor,
        "String Channel parameters": MultipleRowsExcelTabProcessor,
        "Tracker parameters": MultipleRowsExcelTabProcessor,
        "Weather station parameters": MultipleRowsExcelTabProcessor,
        "Sub Station": MultipleRowsExcelTabProcessor,
        "Sub Station Transformer": MultipleRowsExcelTabProcessor,
        "Feeder": MultipleRowsExcelTabProcessor,
        "Grid Connection Point": MultipleRowsExcelTabProcessor,
        "PPC Parameters": MultipleRowsExcelTabProcessor,
        "Meter parameters": MultipleRowsExcelTabProcessor,
    }

    def __init__(self, dwh):
        self.dwh = dwh

    def run(self, output_file_name: str) -> None:
        try:
            excel_file = openpyxl.load_workbook(self.TEMPLATE_NAME)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"The template file has not been found. "
                f"Paste the file named {self.TEMPLATE_NAME} in the same directory as the script."
            )

        self._fill_excel_file(excel_file)

        self._save_excel_sheet(excel_file, output_file_name)
        excel_file.close()

    def _fill_excel_file(self, excel_file: workbook.Workbook) -> None:
        for tab_name in excel_file.sheetnames:
            if tab_name in self.TAB_NAME_TO_PROCESSOR_MAPPING:
                logging.info(f"Checking if the tab named {tab_name!r} needs data...")
                self._fill_tab_with_data(excel_file, tab_name)
                logging.info(f"The tab named {tab_name!r} has been filled with data.")
            else:
                logging.info(f"The tab named {tab_name!r} has been skipped.")

    def _fill_tab_with_data(self, excel_file: workbook.Workbook, tab_name: str) -> None:
        data_to_write = self.dwh.get_data_for_tab(tab_name)

        row_count = len(data_to_write)
        logging.info(f"Number of rows to write for {tab_name!r}: {row_count}")

        tab_processor = self.__select_tab_processor(data_to_write, excel_file, tab_name)

        if row_count == 0:
            tab_processor.clear_data_from_tab()
        else:
            tab_processor.run_tab_processing()

    def __select_tab_processor(
        self, data_to_write: list[Any], excel_file: workbook.Workbook, tab_name: str
    ) -> BaseExcelTabProcessor:
        tab_processor_class = self.TAB_NAME_TO_PROCESSOR_MAPPING[tab_name]
        tab_processor = tab_processor_class(data_to_write, excel_file[tab_name])
        return tab_processor

    def _save_excel_sheet(
        self, excel_sheet: workbook.Workbook, output_file_name: str
    ) -> None:
        logging.info(f"Saving the Excel sheet to {output_file_name!r}...")
        excel_sheet.save(output_file_name)
        logging.info("Saving the Excel sheet has been completed!")


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
        self.queries = QUERIES_LIST

    def __enter__(self):
        self.__connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.__disconnect()

    def get_data_for_tab(self, tab_name: str) -> list[tuple]:
        query = self.queries[tab_name].format(self.site)
        result = self.fetch(query=query)
        return result

    def get_query(self, query_name: str) -> str:
        return self.queries[query_name].format(self.site)

    def fetch(self, query: str) -> List[Any]:
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

    def __get_list_of_supported_pyodbc_drivers(self) -> List[Any]:
        return pyodbc.drivers()

    def __get_list_of_available_and_supported_pyodbc_drivers(self) -> List[Any]:
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


def as_text(value) -> str:
    if value is None:
        return ""
    return str(value)


def get_excel_column_names_iterator() -> Iterator:
    uppercase_aa_zz_list = list(ascii_uppercase) + [
        x[0] + x[1] for x in combinations_with_replacement(ascii_uppercase, 2)
    ]
    return iter(uppercase_aa_zz_list)


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


if __name__ == "__main__":
    # parse arguments from command line:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db_url",
        type=str,
        default="localhost",
        help="The URL of the MSSQL server",
    )
    parser.add_argument(
        "--db_name",
        type=str,
        default="master",
        help="The name of the database",
    )
    parser.add_argument(
        "--db_username",
        type=str,
        default="sa",
        help="The username",
    )
    parser.add_argument(
        "--db_password",
        type=str,
        default="Password123",
        help="The password",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default="output.xlsx",
        help="The name of the output file",
    )
    parser.add_argument(
        "--site",
        type=str,
        default="None",
        help="Site to fetch data for",
    )
    args = parser.parse_args()
    output_file = args.output_file
    db_url = args.db_url
    db_name = args.db_name
    db_username = args.db_username
    db_password = args.db_password
    site = args.site

    data_warehouse = Dwh(
        url=db_url,
        database=db_name,
        username=db_username,
        password=db_password,
        site=site,
    )
    DwhExcelSheetsFiller(dwh=data_warehouse).run(output_file_name=output_file)
