import logging
import os
import re

import openpyxl

from script_components import mappings

logging.basicConfig(level=logging.INFO)


class SQLStoreProcedureUpdater:
    TEMPLATE_NAME = "template.xlsx"
    EXCEL_TABS_TO_COLLECT_COLUMNS = [
        "Main plant parameters",
        "DWH Extract Parameters",
        "UA model parameters",
        "SolarGIS parameters",
        "Sub facilities",
        "String combiner parameters",
        "Transformer parameters",
        "Module-Mounting stru parameters",
        "Inverter parameters",
        "String Channel parameters",
        "Tracker parameters",
        "Weather station parameters",
        "Sub Station",
        "Sub Station Transformer",
        "Feeder",
        "Grid Connection Point",
        "PPC Parameters",
        "Meter parameters",
    ]
    PROCEDURE_REGEX = re.compile(r"if @DataType='(.+)' or @DataType is null\n")

    def __init__(self, procedure_sql_file: str):
        self.procedure_sql_file = procedure_sql_file

    def update_with_column_names(self, output_file: str):
        logging.info("The stored procedure update has been started!")
        logging.info("Collecting columns from Excel file...")
        procedure_to_columns_mapping = self._collect_columns_mapping()
        logging.info("Creating the output SQL file...")
        self._create_sql_output_file(
            procedure_sql_file, output_file, procedure_to_columns_mapping
        )
        logging.info(f"The stored procedure update has been done! The new file is: {output_file}.")

    def _collect_columns_mapping(self):
        excel_file = openpyxl.load_workbook("template.xlsx")

        procedure_to_columns_mapping = {}
        for tab in excel_file.sheetnames:
            if tab in self.EXCEL_TABS_TO_COLLECT_COLUMNS:
                column_extractor = mappings.TAB_NAME_TO_EXTRACTOR_MAPPING[tab]()
                sheet = excel_file[tab]
                column_names = column_extractor.extract_dwh_column_names(sheet)
                procedure_to_columns_mapping[tab] = column_names

        return procedure_to_columns_mapping

    def _create_sql_output_file(
        self,
        procedure_sql_file: str,
        output_file: str,
        procedure_to_columns_mapping: dict[str, list[str]],
    ):
        output_sql_file_lines = []
        current_procedure_updater = None
        with open(procedure_sql_file, "r") as file:
            for line in file:
                if current_procedure_updater:
                    processed_line = current_procedure_updater.process_file_line(line)
                    self._add_line_to_output_file_content(
                        output_sql_file_lines, processed_line
                    )

                    if current_procedure_updater.should_be_terminated:
                        current_procedure_updater = None
                else:
                    procedure_name = self._check_if_procedure_opening_line(line)
                    if (
                        procedure_name == "String combiner"
                    ):  # String combiner Excel tab name differs from DWH stored procedure @DataType
                        procedure_name = "String combiner parameters"

                    if procedure_name:
                        stored_procedure_updater_class = mappings.STORED_PROCEDURE_VARIANT_NAME_TO_STORED_PROCEDURE_UPDATER_MAPPING[
                            procedure_name
                        ]
                        column_names = procedure_to_columns_mapping[procedure_name]
                        current_procedure_updater = stored_procedure_updater_class(
                            column_names=column_names
                        )
                        self._add_line_to_output_file_content(
                            output_sql_file_lines, line
                        )
                    else:
                        self._add_line_to_output_file_content(
                            output_sql_file_lines, line
                        )

        self._save_sql_output_file(output_file, output_sql_file_lines)

    def _add_line_to_output_file_content(
        self, output_sql_file_lines: list[str], line: str
    ) -> None:
        output_sql_file_lines.append(line)

    def _check_if_procedure_opening_line(self, line: str) -> str | None:
        match_object = self.PROCEDURE_REGEX.match(line)
        if not match_object:
            return None
        return match_object.group(1)

    def _save_sql_output_file(
        self, output_file: str, output_sql_file_lines: list[str]
    ) -> None:
        logging.info("Saving the SQL output file...")
        with open(output_file, "w") as file:
            file.writelines(output_sql_file_lines)


if __name__ == "__main__":
    procedure_sql_file = os.getenv("PROCEDURE_SQL_FILE")
    output_file = os.getenv("OUTPUT_FILE")

    updater = SQLStoreProcedureUpdater(procedure_sql_file)
    updater.update_with_column_names(output_file)
