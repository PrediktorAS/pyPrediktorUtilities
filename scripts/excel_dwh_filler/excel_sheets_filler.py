import argparse
import logging
from typing import Any

import openpyxl
from openpyxl import workbook

from script_components import excel_sheet_dwh
from script_components.tab_processors import basic as basic_processors, specified as specified_processors

logging.basicConfig(level=logging.INFO)


class DwhExcelSheetsFiller:
    TEMPLATE_NAME = "template.xlsx"
    TAB_NAME_TO_PROCESSOR_MAPPING = {
        "Main plant parameters": specified_processors.MainPlantParametersExcelTabProcessor,
        "DWH Extract Parameters": specified_processors.DWHExtractParametersExcelTabProcessor,
        "UA model parameters": specified_processors.UAModelParametersExcelTabProcessor,
        "SolarGIS parameters": specified_processors.SolarGISParametersExcelTabProcessor,
        "Sub facilities": basic_processors.MultipleRowsExcelTabProcessor,
        "String combiner parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Transformer parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Module-Mounting stru parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Inverter parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "String Channel parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Tracker parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Weather station parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Sub Station": basic_processors.MultipleRowsExcelTabProcessor,
        "Sub Station Transformer": basic_processors.MultipleRowsExcelTabProcessor,
        "Feeder": basic_processors.MultipleRowsExcelTabProcessor,
        "Grid Connection Point": basic_processors.MultipleRowsExcelTabProcessor,
        "PPC Parameters": basic_processors.MultipleRowsExcelTabProcessor,
        "Meter parameters": basic_processors.MultipleRowsExcelTabProcessor,
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
    ) -> basic_processors.BaseExcelTabProcessor:
        tab_processor_class = self.TAB_NAME_TO_PROCESSOR_MAPPING[tab_name]
        tab_processor = tab_processor_class(data_to_write, excel_file[tab_name])
        return tab_processor

    def _save_excel_sheet(
        self, excel_sheet: workbook.Workbook, output_file_name: str
    ) -> None:
        logging.info(f"Saving the Excel sheet to {output_file_name!r}...")
        excel_sheet.save(output_file_name)
        logging.info("Saving the Excel sheet has been completed!")


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

    data_warehouse = excel_sheet_dwh.Dwh(
        url=db_url,
        database=db_name,
        username=db_username,
        password=db_password,
        site=site,
    )
    DwhExcelSheetsFiller(dwh=data_warehouse).run(output_file_name=output_file)
