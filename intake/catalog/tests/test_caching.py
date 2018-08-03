import os.path
import pytest

from intake import Catalog

@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog1.yml'))


def test_cache_catalog(catalog1):
    assert 'cache' in catalog1['simple_cache'].describe_open().keys()

def test_cache_path(catalog1):
    cat = catalog1['http_cache']
    cache = cat.cache[0]

    cache_path = cache._path(cat._urlpath)

    assert cache._cache_dir in cache_path

    filename = os.path.basename(cache_path)
    import string
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in filename)

def test_open_catalog(catalog1):
    ds = catalog1['http_cache'].get()
    schema = ds._get_schema()