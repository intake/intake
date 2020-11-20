#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
import shutil
import string
import time

import intake
from intake.config import conf
from intake.utils import make_path_posix


@pytest.fixture
def catalog_cache():
    path = os.path.dirname(__file__)
    return intake.open_catalog(os.path.join(path, 'catalog_caching.yml'))


def test_load_csv(catalog_cache, tempdir):
    os.environ['TEST_CACHE_DIR'] = str(tempdir)

    catalog_cache['test_cache_new'].read()
    files = os.listdir(tempdir)
    assert 'cache' in files
    assert len(files) == 2
    cache_id = [f for f in files if f != 'cache'][0]
    assert all(c in string.hexdigits for c in cache_id)


def test_list_of_files(catalog_cache):
    pd = pytest.importorskip('pandas')
    s1 = catalog_cache['test_cache']
    s2 = catalog_cache['test_list_cache']
    assert s2.read().equals(pd.concat([s1.read(), s1.read()]))


def test_bad_type_cache(catalog_cache):
    with pytest.raises(IndexError):
        catalog_cache['test_bad_type_cache_spec'].cache


def test_load_textfile(catalog_cache):
    cat = catalog_cache['text_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    assert cache._cache_dir in cache_path
    assert os.path.isfile(cache_path)

    cache_id = os.path.basename(os.path.dirname(cache_path))
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)
    cache.clear_all()


def test_load_arr(catalog_cache):
    cat = catalog_cache['arr_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat.path, output=False)
    cache_path = cache_paths[-1]

    assert cache._cache_dir in cache_path
    assert os.path.isfile(cache_path)

    cache_id = os.path.basename(os.path.dirname(cache_path))
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)
    cache.clear_all()


@pytest.mark.parametrize('section', ['test_no_regex',
                                     'test_regex_no_match',
                                     'test_regex_partial_match'])
def test_regex(catalog_cache, section):
    cat = catalog_cache[section]
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    assert cache_path.startswith(cache._cache_dir)
    assert os.path.isfile(cache_path)

    cache.clear_all()


def test_get_metadata(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]
    cache_paths = cache.load(cat._urlpath, output=False)

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
    cache_paths = cache.load(cat._urlpath, output=False)

    cache.clear_cache(cat._urlpath)

    assert cat._urlpath not in cache._metadata.keys()
    for cache_path in cache_paths:
        assert os.path.basename(cache_path) not in os.listdir(cache._cache_dir)


def test_clear_cache_bad_metadata(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]
    cache_paths = cache.load(cat._urlpath, output=False)

    subdir = os.path.dirname(cache_paths[0])

    shutil.rmtree(subdir)

    cache.clear_cache(cat._urlpath)

    assert cat._urlpath not in cache._metadata.keys()
    for cache_path in cache_paths:
        assert os.path.basename(cache_path) not in os.listdir(cache._cache_dir)


def test_clear_all(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]
    cache_paths = cache.load(cat._urlpath, output=False)

    cache.clear_all()

    for cache_path in cache_paths:
        assert not os.path.exists(cache_path)

    cache.clear_all()


def test_second_load(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    assert os.path.isfile(cache_path)
    t1 = os.path.getmtime(cache_path)

    cache.load(cat._urlpath, output=False)
    assert os.path.isfile(cache_path)
    t2 = os.path.getmtime(cache_path)
    assert t1 == t2

    cache.clear_all()


def test_second_load_timestamp(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    time1 = os.path.getmtime(cache_path)

    cache.clear_cache(cat._urlpath)
    assert not os.path.isfile(cache_path)
    time.sleep(0.5)

    cache.load(cat._urlpath, output=False)
    assert os.path.isfile(cache_path)

    time2 = os.path.getmtime(cache_path)
    assert time1 < time2

    cache.clear_all()


def test_second_load_refresh(catalog_cache):
    cat = catalog_cache['test_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    time1 = os.path.getmtime(cache_path)

    assert os.path.isfile(cache_path)

    cache.load(cat._urlpath, output=False)
    assert os.path.isfile(cache_path)

    time2 = os.path.getmtime(cache_path)
    assert time1 == time2

    cache.clear_all()


def test_multiple_cache(catalog_cache):
    cat = catalog_cache['test_multiple_cache']

    assert len(cat.cache) == 2

    for cache in cat.cache:

        cache_paths = cache.load(cat._urlpath, output=False)
        cache_path = cache_paths[-1]

        assert cache._cache_dir in cache_path
        assert os.path.isfile(cache_path)

        cache.clear_all()


def test_disable_caching(catalog_cache):
    conf['cache_disabled'] = True

    cat = catalog_cache['test_cache']
    cache = cat.cache[0]

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    assert cache_path == cat._urlpath

    conf['cache_disabled'] = False

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]

    assert cache._cache_dir in cache_path
    assert os.path.isfile(cache_path)

    cache_id = os.path.basename(os.path.dirname(cache_path))
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)
    cache.clear_all()


def test_ds_set_cache_dir(catalog_cache):
    cat = catalog_cache['test_cache']()
    defaults = cat.cache_dirs

    new_cache_dir = os.path.join(os.getcwd(), 'test_cache_dir')
    cat.set_cache_dir(new_cache_dir)

    cache = cat.cache[0]
    assert make_path_posix(cache._cache_dir) == make_path_posix(new_cache_dir)

    cache_paths = cache.load(cat._urlpath, output=False)
    cache_path = cache_paths[-1]
    expected_cache_dir = make_path_posix(new_cache_dir)

    assert expected_cache_dir in cache_path
    assert defaults[0] not in cache_path
    assert os.path.isfile(cache_path)

    cache_id = os.path.basename(os.path.dirname(cache_path))
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in cache_id)
    cache.clear_all()

    shutil.rmtree(expected_cache_dir)
