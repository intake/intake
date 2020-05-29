from intake.catalog.base import Catalog


def test_no_entry():
    cat = Catalog()
    cat2 = cat.configure_new()
    assert isinstance(cat2, Catalog)
