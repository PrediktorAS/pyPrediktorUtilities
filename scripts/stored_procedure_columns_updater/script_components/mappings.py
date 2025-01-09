from script_components.column_extractors import specified as specified_column_extractors
from script_components.stored_procedure_updaters import (
    basic as basic_stored_procedure_updaters,
    specified as specified_stored_procedure_updaters,
)

TAB_NAME_TO_EXTRACTOR_MAPPING = {
    "Main plant parameters": specified_column_extractors.MainPlantParametersColumnExtractor,
    "DWH Extract Parameters": specified_column_extractors.DWHExtractParametersColumnExtractor,
    "UA model parameters": specified_column_extractors.UAModelParametersColumnExtractor,
    "SolarGIS parameters": specified_column_extractors.SolarGISParametersColumnExtractor,
    "Sub facilities": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "String combiner parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Transformer parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Module-Mounting stru parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Inverter parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "String Channel parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Tracker parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Weather station parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Sub Station": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Sub Station Transformer": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Feeder": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Grid Connection Point": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "PPC Parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
    "Meter parameters": specified_column_extractors.DwhColumnsFromExcelRowExtractor,
}

STORED_PROCEDURE_VARIANT_NAME_TO_STORED_PROCEDURE_UPDATER_MAPPING = {
    "Main plant parameters": specified_stored_procedure_updaters.MainPlantParametersStoredProcedureUpdater,
    "DWH Extract Parameters": basic_stored_procedure_updaters.Option3And4StoredProcedureUpdater,
    "UA model parameters": basic_stored_procedure_updaters.Option3And4StoredProcedureUpdater,
    "SolarGIS parameters": basic_stored_procedure_updaters.Option3And4StoredProcedureUpdater,
    "Sub facilities": specified_stored_procedure_updaters.SubFacilitiesStoredProcedureUpdater,
    "String combiner parameters": specified_stored_procedure_updaters.StringCombinerStoredProcedureUpdater,
    "Transformer parameters": basic_stored_procedure_updaters.Option5And6StoredProcedureUpdater,
    "Module-Mounting stru parameters": basic_stored_procedure_updaters.Option5And6StoredProcedureUpdater,
    "Inverter parameters": specified_stored_procedure_updaters.InverterParametersStoredProcedureUpdater,
    "String Channel parameters": specified_stored_procedure_updaters.StringChannelParametersStoredProcedureUpdater,
    "Tracker parameters": specified_stored_procedure_updaters.TrackerParametersStoredProcedureUpdater,
    "Weather station parameters": specified_stored_procedure_updaters.WeatherStationParametersStoredProcedureUpdater,
    "Sub Station": specified_stored_procedure_updaters.SubStationStoredProcedureUpdater,
    "Sub Station Transformer": specified_stored_procedure_updaters.SubStationTransformerStoredProcedureUpdater,
    "Feeder": basic_stored_procedure_updaters.Option7And10StoredProcedureUpdater,
    "Grid Connection Point": basic_stored_procedure_updaters.Option7And10StoredProcedureUpdater,
    "PPC Parameters": specified_stored_procedure_updaters.PPCParametersStoredProcedureUpdater,
    "Meter parameters": basic_stored_procedure_updaters.Option3And4StoredProcedureUpdater,
}
