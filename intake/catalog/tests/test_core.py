import pytest
from intake.catalog.base import Catalog


def test_no_entry():
    cat = Catalog()
    cat2 = cat.configure_new()
    assert isinstance(cat2, Catalog)
    assert cat.auth is None
    assert cat2.auth is None


def test_regression():
    with pytest.raises(ValueError):
        Catalog("URI")
