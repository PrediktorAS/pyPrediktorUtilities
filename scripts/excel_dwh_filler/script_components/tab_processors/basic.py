import abc
import copy
import datetime
import logging
from typing import Any

from openpyxl.styles import numbers
from openpyxl.worksheet import worksheet

from script_components import helpers


class BaseExcelTabProcessor(abc.ABC):
    DATE_FORMAT = "%Y-%m-%d"
    DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    PYTHON_TYPE_TO_EXCEL_TYPE = {
        "str": numbers.FORMAT_TEXT,
        "int": numbers.FORMAT_NUMBER,
        "float": numbers.FORMAT_NUMBER_COMMA_SEPARATED1,
    }

    def __init__(self, data: list[Any], tab: worksheet.Worksheet):
        self.data = data
        self.tab = tab

    @abc.abstractmethod
    def run_tab_processing(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def clear_data_from_tab(self) -> None:
        raise NotImplementedError

    def _determine_data_type_and_value(self, value) -> dict:
        data_type = None

        match value:
            case bool():
                value = int(value)
                data_type = "int"
            case float():
                if value.is_integer():
                    value = int(value)
                    data_type = "int"
                else:
                    value = helpers.as_text(value)
                    data_type = "float"
            case datetime.datetime():
                value = value.strftime(self.DATE_TIME_FORMAT)
                data_type = "str"
            case datetime.date():
                value = value.strftime(self.DATE_FORMAT)
                data_type = "str"

        if data_type is None:
            data_type = "str"

        return {"data_type": data_type, "value": value}

    def _place_value_at_location(self, cell_location: str, row_data: dict) -> None:
        """
        Places the value in the Excel tab at the specified cell location.
        Example:
            cell_location = "A1"
            row_data = {"data_type": "int", "value": "123"}
        """
        formatted_row_value = helpers.as_text(row_data["value"])
        cell = self.tab[cell_location]
        cell.value = formatted_row_value

        if row_data["data_type"] in ["int", "float"]:
            number_data_format = "n"
            cell.data_type = number_data_format

        cell.number_format = self.PYTHON_TYPE_TO_EXCEL_TYPE[row_data["data_type"]]


class SingleColumnExcelTabProcessor(BaseExcelTabProcessor):
    VALUE_COLUMN = "B"
    FIRST_ROW_WITH_VALUE_TO_FILL = 1

    def __init__(self, data: list[Any], tab: worksheet.Worksheet):
        super().__init__(data, tab)
        if not data:
            raise ValueError("The data is missing for the requested site.")

        self.data_tuple = tuple(data[0])

    def run_tab_processing(self) -> None:
        self._fill_excel_column_with_values()

    def _fill_excel_column_with_values(self) -> None:
        excel_row_index = copy.copy(self.FIRST_ROW_WITH_VALUE_TO_FILL)

        for row_datum in self.data_tuple:
            transformed_row_data = self._determine_data_type_and_value(row_datum)
            cell_location = f"{self.VALUE_COLUMN}{excel_row_index}"
            self._place_value_at_location(cell_location, transformed_row_data)

            excel_row_index += 1

    def clear_data_from_tab(self) -> None:
        logging.warning(
            "Clearing data for the single-column values tabs not implemented. "
            "If you can see this, the implementation should be added!"
        )


class MultipleRowsExcelTabProcessor(BaseExcelTabProcessor):
    FIRST_ROW_TO_FILL = 3

    def run_tab_processing(self) -> None:
        self._fill_excel_rows_with_values()

    def _fill_excel_rows_with_values(self) -> None:
        excel_row_index = copy.copy(self.FIRST_ROW_TO_FILL)

        for row_data in self.data:
            excel_column_names_iterator = helpers.get_excel_column_names_iterator()
            for cell_datum in row_data:
                column_index = next(excel_column_names_iterator)
                cell_location = f"{column_index}{excel_row_index}"

                transformed_cell_datum = self._determine_data_type_and_value(cell_datum)
                self._place_value_at_location(cell_location, transformed_cell_datum)
            excel_row_index += 1

    def clear_data_from_tab(self) -> None:
        for row in self.tab.iter_rows(min_row=self.FIRST_ROW_TO_FILL):
            for cell in row:
                cell.value = None
