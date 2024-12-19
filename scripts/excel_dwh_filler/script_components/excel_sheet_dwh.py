import logging
from typing import Any

from pyprediktorutilities.dwh.dwh_singleton import DwhSingleton

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


class ExcelSheetDwh(DwhSingleton):
    def get_data_for_tab(self, tab_name: str, site: str) -> list[Any]:
        query = QUERIES_LIST[tab_name].format(site)
        result = self.execute(query=query)

        if not result:
            logging.warning(
                f"No data fetched for tab: {tab_name!r} for site: {site!r}."
            )
        return result
