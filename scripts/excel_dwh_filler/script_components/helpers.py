import itertools
import string

from typing import Iterator


def as_text(value) -> str:
    if value is None:
        return ""
    return str(value)


def get_excel_column_names_iterator() -> Iterator:
    single_letters = list(string.ascii_uppercase)
    double_letters = []
    for letter in single_letters:
        uppercase_aa_zz_list = [
            f"{letter}{second_letter}" for second_letter in single_letters
        ]
        double_letters = [*double_letters, *uppercase_aa_zz_list]
    return itertools.chain(single_letters, double_letters)
