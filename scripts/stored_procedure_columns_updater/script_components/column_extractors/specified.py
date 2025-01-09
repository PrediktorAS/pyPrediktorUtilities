import re

from openpyxl.worksheet import worksheet

from script_components.column_extractors import basic


class MainPlantParametersColumnExtractor(basic.DwhColumnsFromExcelColumnExtractor):
    COLUMN_WITH_COLUMN_NAMES = "A"
    STARTING_COLUMN = 4
    ROUND_BRACES_REGEX = re.compile(r" \(.+\)")

    COLUMN_NAMES_TO_SKIP = [
        "Plant contruction:",
        "Dates:",
        "Nominal values and KPI calculation parameters:",
        "Plant and equipment model parameters:",
        "Location and contacts:",
        "Soiling data:",
        "Other parameters:",
        "Model tag names:",
        "Model calculation parameters",
        "",
    ]


class DWHExtractParametersColumnExtractor(basic.DwhColumnsFromExcelColumnExtractor):
    STARTING_COLUMN = 3


class UAModelParametersColumnExtractor(basic.DwhColumnsFromExcelColumnExtractor):
    STARTING_COLUMN = 3


class SolarGISParametersColumnExtractor(basic.DwhColumnsFromExcelColumnExtractor):
    STARTING_COLUMN = 5


class DwhColumnsFromExcelRowExtractor(basic.BaseDwhColumnExtractor):
    ROW_WITH_COLUMN_NAMES_INDEX = 1

    def extract_dwh_column_names(self, sheet: worksheet.Worksheet) -> list[str]:
        dwh_column_names = []

        first_row = sheet[self.ROW_WITH_COLUMN_NAMES_INDEX]
        for cell in first_row:
            dwh_column_name = self._format_dwh_column_name(cell.value)
            if dwh_column_name:
                dwh_column_names.append(dwh_column_name)

        return dwh_column_names
