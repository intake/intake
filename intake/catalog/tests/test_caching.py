import os.path
import pytest

from intake import Catalog

@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog1.yml'))


def test_cache_catalog(catalog1):
    assert 'cache' in catalog1['simple_cache'].describe_open().keys()


def test_open_catalog(catalog1):
    ds = catalog1['simple_cache'].get()