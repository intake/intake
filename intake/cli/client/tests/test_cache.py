#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import intake
import intake.config
from intake.source.cache import CacheMetadata
import os
import pytest
import subprocess
import sys
from intake.utils import make_path_posix
cpath = make_path_posix(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__),
                     '..', '..', '..',
                     'catalog', 'tests', 'catalog_caching.yml')))


@pytest.mark.skipif(sys.version_info[0] == 2,
                    reason="Py2 exists early on argparse")
def test_help(temp_cache, env):
    out = subprocess.check_output(['intake', 'cache'],
                                  env=env, universal_newlines=True)
    assert out.startswith('usage: ')

    out2 = subprocess.check_output(['intake', 'cache', '-h'],
                                   env=env, universal_newlines=True)
    assert out2 == out


def test_list_keys(temp_cache, env):
    out = subprocess.check_output(['intake', 'cache', 'list-keys'],
                                  env=env, universal_newlines=True)
    assert out.startswith('[]')  # empty cache
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    out = subprocess.check_output(['intake', 'cache', 'list-keys'],
                                  env=env, universal_newlines=True)
    assert 'states.csv' in out


def test_precache(temp_cache, env):
    out = subprocess.check_output(['intake', 'cache', 'list-keys'],
                                  env=env, universal_newlines=True)
    assert out.startswith('[]')  # empty cache
    out = subprocess.check_output(['intake', 'precache', cpath],
                                  env=env, universal_newlines=True)
    assert out.count('Caching for entry') > 1
    out = subprocess.check_output(['intake', 'cache', 'list-keys'],
                                  env=env, universal_newlines=True)
    assert 'states.csv' in out
    assert 'small.npy' in out


def test_clear_all(temp_cache, env):
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    md = CacheMetadata()
    assert len(md) == 1
    assert 'states' in list(md)[0]
    subprocess.call(['intake', 'cache', 'clear'], env=env)
    md = CacheMetadata()
    assert len(md) == 0


def test_clear_one(temp_cache, env):
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    cat.arr_cache.read()
    md = CacheMetadata()
    keys = list(md)
    assert len(keys) == 2
    subprocess.call(['intake', 'cache', 'clear', keys[0]],
                    env=env)
    md = CacheMetadata()
    assert len(md) == 1
    assert list(md)[0] == keys[1]


def test_usage(temp_cache, env):
    from intake.source.cache import BaseCache
    BaseCache(None, None).clear_all()
    out = subprocess.check_output(['intake', 'cache', 'usage'],
                                  env=env, universal_newlines=True)
    assert '0.0' in out  # empty!
