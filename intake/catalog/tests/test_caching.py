import os
import pytest

from intake import Catalog

@pytest.fixture
def catalog_cache():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog_caching.yml'))

def test_load_csv(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath)
    cache_path = cache_paths[-1]
    
    assert cache._cache_dir in cache_path

    cache_id = os.path.basename(os.path.dirname(cache_path))
    import string
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)

def test_load_textfile(catalog_cache):
    cat = catalog_cache['text_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath)
    cache_path = cache_paths[-1]
    
    assert cache._cache_dir in cache_path

    cache_id = os.path.basename(os.path.dirname(cache_path))
    import string
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)

def test_load_arr(catalog_cache):
    cat = catalog_cache['arr_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat.path)
    cache_path = cache_paths[-1]
    
    assert cache._cache_dir in cache_path

    cache_id = os.path.basename(os.path.dirname(cache_path))
    import string
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)

def test_get_metadata(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]
    cache_paths = cache.load(cat._urlpath)

    metadata = cache.get_metadata(cat._urlpath)

    assert isinstance(metadata, list)
    for d in metadata:
        assert d['cache_path'] in cache_paths
        assert 'created' in d.keys()
        assert 'original_path' in d.keys()

def test_clear_cache(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]
    cache_paths = cache.load(cat._urlpath)

    cache.clear_cache(cat._urlpath)

    assert cat._urlpath not in cache._metadata._metadata.keys()
    for cache_path in cache_paths:
        assert os.path.basename(cache_path) not in os.listdir(cache._cache_dir)

def test_clear_all(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]
    cache.load(cat._urlpath)

    cache.clear_all()

    assert not os.path.exists(cache._cache_dir)