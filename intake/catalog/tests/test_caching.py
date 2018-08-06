import os
import pytest

from intake import Catalog

@pytest.fixture
def catalog_cache():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog_caching.yml'))


def test_simple_cache(catalog_cache):
    assert 'cache' in catalog_cache['simple_cache'].describe_open().keys()

def test_load(catalog_cache):
    cat = catalog_cache['http_cache']
    cache = cat.cache[0]

    cache_path = cache.load(cat._urlpath)
    
    assert cache._cache_dir in cache_path

    cache_id = os.path.basename(cache_path)
    import string
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)

def test_get_metadata(catalog_cache):
    cat = catalog_cache['http_cache']
    cache = cat.cache[0]
    cache_path = cache.load(cat._urlpath)

    cache_id = os.path.basename(cache_path)

    metadata = cache.get_metadata(cache_id)

    assert cat._urlpath == metadata['urlpath']
    assert 'created' in metadata.keys()

def test_clear_cache(catalog_cache):
    cat = catalog_cache['http_cache']
    cache = cat.cache[0]
    cache_path = cache.load(cat._urlpath)
    cache_id = os.path.basename(cache_path)

    cache.clear_cache(cache_id)

    assert cache_id not in cache._metadata._metadata.keys()
    assert cache_id not in os.listdir(cache._cache_dir)

def test_clear_all(catalog_cache):
    cat = catalog_cache['http_cache']
    cache = cat.cache[0]
    cache.load(cat._urlpath)

    cache.clear_all()

    assert not os.path.exists(cache._cache_dir)