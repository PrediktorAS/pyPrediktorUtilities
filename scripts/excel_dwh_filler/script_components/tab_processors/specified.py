import copy
from typing import Any

from openpyxl.worksheet import worksheet

from script_components.tab_processors import basic as basic_processors


class MainPlantParametersExcelTabProcessor(
    basic_processors.SingleColumnExcelTabProcessor
):
    FIRST_ROW_WITH_VALUE_TO_FILL = 4

    TARGET_DWH_OUTPUT_LENGTH = 135
    INDEX_PRIOR_TO_MISSING_GRID_STATION_ID = 14

    def __init__(self, data: list[Any], tab: worksheet.Worksheet):
        super().__init__(data, tab)
        self.is_missing_grid_station_id = False

    def _fill_excel_column_with_values(self) -> None:
        self.is_missing_grid_station_id = (
            len(self.data_tuple) == self.TARGET_DWH_OUTPUT_LENGTH
        )
        excel_row_index = copy.copy(self.FIRST_ROW_WITH_VALUE_TO_FILL)

        for row_datum in self.data_tuple:
            transformed_row_data = self._determine_data_type_and_value(row_datum)
            cell_location = f"{self.VALUE_COLUMN}{excel_row_index}"
            self._place_value_at_location(cell_location, transformed_row_data)

            if self.is_missing_grid_station_id:
                if excel_row_index == self.INDEX_PRIOR_TO_MISSING_GRID_STATION_ID:
                    excel_row_index += 1
                    self.tab[f"{self.VALUE_COLUMN}{excel_row_index}"] = ""

            excel_row_index += 1


class DWHExtractParametersExcelTabProcessor(
    basic_processors.SingleColumnExcelTabProcessor
):
    FIRST_ROW_WITH_VALUE_TO_FILL = 3


class UAModelParametersExcelTabProcessor(
    basic_processors.SingleColumnExcelTabProcessor
):
    FIRST_ROW_WITH_VALUE_TO_FILL = 3


class SolarGISParametersExcelTabProcessor(
    basic_processors.SingleColumnExcelTabProcessor
):
    FIRST_ROW_WITH_VALUE_TO_FILL = 5
