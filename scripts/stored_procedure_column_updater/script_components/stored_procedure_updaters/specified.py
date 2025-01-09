import re

from script_components.stored_procedure_updaters import basic

class MainPlantParametersStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    REGEXES_TO_CHECK = {
        "option_1": {
            "regex": re.compile(r"(.+)(isnull)(.+)( as )(.+)(,)$"),
            "re_sub_string": r"\1\2\3\4{}\6",
        },
        "option_2": {
            "regex": re.compile(r"(.+)('')(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
            "if_no_column_name": r"\1\2\3",
        },
        "option_3": {
            "regex": re.compile(r"(.+)?(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2\3 as {}\4",
        },
        "option_4": {
            "regex": re.compile(r"(.+)(isnull)(.+)"),
            "re_sub_string": r"\1\2\3 as {}",
        },
    }


class SubFacilitiesStoredProcedureUpdater(basic.Option3And4StoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "from DimSubFacility\n"


class StringCombinerStoredProcedureUpdater(basic.Option5And6StoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "from DimStringMonitor dsm join\n"


class InverterParametersStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "from DimInverter di left join\n"
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_8": {
            "regex": re.compile(r"(.+)(end)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_9": {
            "regex": re.compile(r"('')(,)$"),
            "re_sub_string": r"\1 as {}\2",
            "if_no_column_name": r"\1\2",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
    }


class StringChannelParametersStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "from DimStringMonitor dsm join\n"
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_8": {
            "regex": re.compile(r"(.+)(end)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_9": {
            "regex": re.compile(r"('')(,)$"),
            "re_sub_string": r"\1 as {}\2",
            "if_no_column_name": r"\1\2",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
        "option_11": {
            "regex": re.compile(r"('')(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
            "if_no_column_name": r"\1\2\3",
        },
        "option_16": {
            "regex": re.compile(r"(.+)(,)$"),
            "re_sub_string": r"\1 as {}\2",
        },
    }


class TrackerParametersStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "from DimTracker dt join\n"
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_9": {
            "regex": re.compile(r"('')(,)$"),
            "re_sub_string": r"\1 as {}\2",
            "if_no_column_name": r"\1\2",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
    }


class WeatherStationParametersStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_8": {
            "regex": re.compile(r"(.+)(end)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_9": {
            "regex": re.compile(r"('')(,)$"),
            "re_sub_string": r"\1 as {}\2",
            "if_no_column_name": r"\1\2",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
    }


class SubStationStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
        "option_12": {
            "regex": re.compile(r"(.+)(,)$"),
            "re_sub_string": r"\1 as {} \2",
        },
    }


class SubStationTransformerStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "end\n"
    REGEXES_TO_CHECK = {
        "option_4": {
            "regex": re.compile(r"(.+)(isnull)(.+)"),
            "re_sub_string": r"\1\2\3 as {}",
        },
        "option_13": {
            "regex": re.compile(r"(FacilityName+'-USS-TR1')"),
            "re_sub_string": r"\1 as {}",
        },
        "option_14": {
            "regex": re.compile(r"(,)('')"),
            "re_sub_string": r"\1\2 as {}",
            "if_no_column_name": r"\1\2",
        },
        "option_15": {
            "regex": re.compile(r"(,)(')(.+)(')"),
            "re_sub_string": r"\1\2\3\4 as {}",
        },
        "option_17": {
            "regex": re.compile(r"(FacilityName\+'-USS-TR1')"),
            "re_sub_string": r"\1 as {}",
        },
    }
    MAX_COLUMN_INDEX = 13

    def process_file_line(self, raw_line: str) -> str:
        for _, regex_properties in self.REGEXES_TO_CHECK.items():
            match_object = regex_properties["regex"].search(raw_line)
            if match_object:
                if self.column_name_index > self.MAX_COLUMN_INDEX:
                    self.column_name_index = 0

                column_name_index = self.column_name_index
                column_name = self.column_names[column_name_index]

                self.column_name_index += 1
                return self._replace_column_name_in_line(
                    raw_line, regex_properties, column_name
                )

        if raw_line == self.PROCEDURE_END_LINE_CONTENT:
            self.should_be_terminated = True
        return raw_line


class PPCParametersStoredProcedureUpdater(basic.BaseStoredProcedureUpdater):
    PROCEDURE_END_LINE_CONTENT = "From DimPPC dppc left join\n"
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_9": {
            "regex": re.compile(r"('')(,)$"),
            "re_sub_string": r"\1 as {}\2",
            "if_no_column_name": r"\1\2",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
    }
