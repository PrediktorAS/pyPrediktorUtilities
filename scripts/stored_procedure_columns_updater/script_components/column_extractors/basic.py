import abc
import re

from openpyxl.cell import Cell
from openpyxl.worksheet import worksheet

from script_components.helpers import as_text


class BaseDwhColumnExtractor(abc.ABC):
    ROUND_BRACES_REGEX = re.compile(r" \(.+\)")
    COLUMN_NAMES_TO_SKIP = []

    @abc.abstractmethod
    def extract_dwh_column_names(self, sheet: worksheet.Worksheet) -> list[str]:
        raise NotImplementedError

    def _format_dwh_column_name(self, cell_value: str | None) -> str:
        should_row_be_skipped = self._check_if_row_should_be_skipped(cell_value)
        if should_row_be_skipped:
            return ""

        cell_value_as_text = as_text(cell_value)
        cell_value_without_colon = cell_value_as_text.removesuffix(":")
        cell_value_without_round_brace_expression = self.ROUND_BRACES_REGEX.sub(
            "", cell_value_without_colon
        )
        cell_value_enclosed_with_double_quotes = (
            f'"{cell_value_without_round_brace_expression}"'
        )
        return cell_value_enclosed_with_double_quotes

    def _check_if_row_should_be_skipped(self, cell_value: str | None) -> bool:
        return cell_value in self.COLUMN_NAMES_TO_SKIP


class DwhColumnsFromExcelColumnExtractor(BaseDwhColumnExtractor):
    COLUMN_WITH_COLUMN_NAMES = "A"
    STARTING_COLUMN = 1

    def extract_dwh_column_names(self, sheet: worksheet.Worksheet) -> list[str]:
        dwh_column_names = []

        for row in sheet.iter_rows(min_row=self.STARTING_COLUMN, max_row=sheet.max_row):
            self._process_excel_row(dwh_column_names, row)
        return dwh_column_names

    def _process_excel_row(self, dwh_column_names: list[str], row: tuple[Cell]) -> None:
        for cell in row:
            is_correct_cell = self._check_if_excel_row_contains_dwh_column_name(
                cell, dwh_column_names
            )
            if is_correct_cell:
                break

    def _check_if_excel_row_contains_dwh_column_name(
        self, cell: Cell, dwh_column_names: list[str]
    ) -> bool:
        is_excel_cell_with_dwh_column_name = (
            cell.column_letter == self.COLUMN_WITH_COLUMN_NAMES
        )
        if is_excel_cell_with_dwh_column_name:
            dwh_column_name = self._format_dwh_column_name(cell.value)
            dwh_column_names.append(dwh_column_name)
        return is_excel_cell_with_dwh_column_name
