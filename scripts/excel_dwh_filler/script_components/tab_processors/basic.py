import abc
import copy
import datetime
import logging

from openpyxl.worksheet import worksheet

from script_components import helpers


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
                    value = helpers.as_text(value)
                    value = self._change_separator_from_dot_to_comma(value)
            case datetime.datetime():
                value = value.strftime(self.DATE_TIME_FORMAT)
            case datetime.date():
                value = value.strftime(self.DATE_FORMAT)
        return value

    def _change_separator_from_dot_to_comma(self, float_as_str: str) -> str:
        float_with_comma_separator = helpers.as_text(float_as_str).replace(".", ",")
        return float_with_comma_separator


class SingleColumnExcelTabProcessor(BaseExcelTabProcessor):
    VALUE_COLUMN = "B"
    FIRST_ROW_WITH_VALUE_TO_FILL = 1

    def __init__(self, data: list[tuple], tab: worksheet.Worksheet):
        super().__init__(data, tab)
        if not data:
            raise ValueError("The data is missing for the requested site.")

        self.data_tuple = data[0]

    def run_tab_processing(self) -> None:
        self._fill_excel_column_with_values()

    def _fill_excel_column_with_values(self) -> None:
        excel_row_index = copy.copy(self.FIRST_ROW_WITH_VALUE_TO_FILL)

        for row_datum in self.data_tuple:
            transformed_row_datum = self._format_not_convertible_to_string_values(
                row_datum
            )
            formatted_row_datum = helpers.as_text(transformed_row_datum)

            cell_location = f"{self.VALUE_COLUMN}{excel_row_index}"
            self.__place_value_at_location(cell_location, formatted_row_datum)

            excel_row_index += 1

    def __place_value_at_location(self, cell_location: str, formatted_row_datum: str):
        self.tab[cell_location] = helpers.as_text(formatted_row_datum)

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

                transformed_cell_datum = self._format_not_convertible_to_string_values(
                    cell_datum
                )
                formatted_cell_datum = helpers.as_text(transformed_cell_datum)
                self.tab[cell_location] = helpers.as_text(formatted_cell_datum)
            excel_row_index += 1

    def clear_data_from_tab(self) -> None:
        # clear the data, but keep the formatting:
        for row in self.tab.iter_rows(min_row=self.FIRST_ROW_TO_FILL):
            for cell in row:
                cell.value = None
