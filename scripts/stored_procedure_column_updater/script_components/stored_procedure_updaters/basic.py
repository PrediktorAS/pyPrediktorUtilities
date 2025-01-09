import re

class BaseStoredProcedureUpdater:
    REGEXES_TO_CHECK = {}
    PROCEDURE_END_LINE_CONTENT = "from\n"

    def __init__(self, column_names: list[str]):
        self.column_names = column_names
        self.should_be_terminated = False
        self.column_name_index = 0

    def process_file_line(self, raw_line: str) -> str:
        for _, regex_properties in self.REGEXES_TO_CHECK.items():
            match_object = regex_properties["regex"].search(raw_line)
            if match_object:
                column_name_index = self.column_name_index

                column_name = self.column_names[column_name_index]
                if column_name == '""':
                    column_name = ""

                self.column_name_index += 1
                return self._replace_column_name_in_line(
                    raw_line, regex_properties, column_name
                )

        if raw_line == self.PROCEDURE_END_LINE_CONTENT:
            self.should_be_terminated = True
        return raw_line

    def _replace_column_name_in_line(
        self, raw_line: str, regex_properties: dict, column_name: str
    ) -> str:
        regex = regex_properties["regex"]
        if column_name:
            re_sub_string = regex_properties["re_sub_string"]
        else:
            re_sub_string = regex_properties["if_no_column_name"]
        return regex.sub(re_sub_string.format(column_name), raw_line)


class Option3And4StoredProcedureUpdater(BaseStoredProcedureUpdater):
    REGEXES_TO_CHECK = {
        "option_3": {
            "regex": re.compile(r"(.+)?(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2\3 as {}\4",
        },
        "option_4": {
            "regex": re.compile(r"(.+)?(isnull)(.+)"),
            "re_sub_string": r"\1\2\3 as {}",
        },
    }


class Option5And6StoredProcedureUpdater(BaseStoredProcedureUpdater):
    REGEXES_TO_CHECK = {
        "option_5": {
            "regex": re.compile(r"(\s+.+)(,)$"),
            "re_sub_string": r"\1 as {},",
        },
        "option_6": {
            "regex": re.compile(r"(\s+)('')"),
            "re_sub_string": r"\1\2 as {}",
            "if_no_column_name": r"\1\2",
        },
    }


class Option7And10StoredProcedureUpdater(BaseStoredProcedureUpdater):
    REGEXES_TO_CHECK = {
        "option_7": {
            "regex": re.compile(r"(isnull)(.+)(,)$"),
            "re_sub_string": r"\1\2 as {}\3",
        },
        "option_10": {
            "regex": re.compile(r"(isnull)(.+)"),
            "re_sub_string": r"\1\2 as {}",
        },
    }
