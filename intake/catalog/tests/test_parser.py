import os.path

import pytest

from intake.catalog import Catalog, exceptions


def abspath(filename):
    return os.path.join(os.path.dirname(__file__), filename)


@pytest.mark.parametrize("filename", [
    "catalog_non_dict",
    "data_source_missing",
    "data_source_name_non_string",
    "data_source_value_non_dict",
    "params_missing_required",
    "params_name_non_string",
    "params_value_bad_choice",
    "params_value_bad_type",
    "params_value_non_dict",
    "plugins_non_dict",
    "plugins_source_both",
    "plugins_source_missing",
    "plugins_source_missing_key",
    "plugins_source_non_dict",
    "plugins_source_non_list",
    "plugins_source_non_string",
])
def test_parser_validation_error(filename):
    with pytest.raises(exceptions.ValidationError):
        catalog = Catalog(abspath(filename + ".yml"))
