import os.path

import pytest

from intake.catalog import Catalog, exceptions


def abspath(filename):
    return os.path.join(os.path.dirname(__file__), filename)


@pytest.mark.parametrize("filename", [
    "plugins_non_dict",
    "plugins_source_both",
    "plugins_source_missing",
    "plugins_source_non_dict",
    "plugins_source_non_list",
    "plugins_source_non_string",
])
def test_parser_plugins_validation_error(filename):
    with pytest.raises(exceptions.ValidationError):
        catalog = Catalog(abspath(filename + ".yml"))
