import sys
from intake.catalog import default
from intake.catalog.base import Catalog


def test_which():
    p = default.which('python')
    assert p == sys.executable


def test_load():
    cat = default.load_user_catalog()
    assert isinstance(cat, Catalog)
    cat = default.load_global_catalog()
    assert isinstance(cat, Catalog)
