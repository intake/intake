import os
import pytest

import intake
from intake.source.cache import FileCache

@pytest.fixture
def catalog_cache():
    path = os.path.dirname(__file__)
    return intake.open_catalog(os.path.join(path, 'catalog_caching.yml'))

@pytest.fixture
def file_cache():
    return FileCache('csv', 
                 {'argkey': 'urlpath', 'regex': 'test/path', 'sub': ''})

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
    cache.clear_all()

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
    cache.clear_all()

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
    cache.clear_all()

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
    cache.clear_all()

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

def test_ensure_cache_dir(file_cache):
    assert not os.path.exists(file_cache._cache_dir)
    file_cache._ensure_cache_dir()
    assert os.path.exists(file_cache._cache_dir)

    file_cache.clear_all()

    with open(file_cache._cache_dir, 'w') as f:
        f.write('')
    
    with pytest.raises(Exception):
        file_cache._ensure_cache_dir()

    os.remove(file_cache._cache_dir)

#TODO:
# [] load (get data, file created)
# [] delete file, load again (get data, file created)
# [] clear cache (file gone), load again (get data, file created with newer timestamp)
# [] load (file created), load again (file updated?); I don't know if "refresh" is a thing yet.
# [] test mutliple cache specs
# [] validation regex
# [] no match in regex is no-op
# [] private methods in Cache class
# [] option (env var?) to ignore caching