import intake
import intake.config
from intake.source.cache import CacheMetadata
import os
import pytest
import subprocess
import sys
from intake.source.tests.util import temp_cache
cpath = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 '../../../catalog/tests/catalog_caching.yml'))


@pytest.mark.skipif(sys.version_info[0] == 2,
                    reason="Py2 exists early on argparse")
def test_help(tempdir):
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache' % tempdir,
                                  shell=True).decode()
    assert 'usage: ' in out

    out2 = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache -h' % tempdir,
                                   shell=True).decode()
    assert out2 == out


def test_list_keys(temp_cache):
    tempdir = intake.config.confdir
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache'
                                  ' list-keys' % tempdir,
                                  shell=True).decode()
    assert '[]\n' in out  # empty cache
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache list-keys'
                                  '' % tempdir,
                                  shell=True).decode()
    assert 'states.csv' in out


def test_precache(temp_cache):
    tempdir = intake.config.confdir
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache list-keys'
                                  '' % tempdir,
                                  shell=True).decode()
    assert out == "[]\n\n"
    out = subprocess.check_output('INTAKE_CONF_DIR=%s INTAKE_CACHE_DIR=%s '
                                  'intake precache %s ' %
                                  (tempdir, tempdir, cpath), shell=True).decode()
    assert out.count('Caching for entry') > 1
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache list-keys'
                                  '' % tempdir,
                                  shell=True).decode()
    assert 'states.csv' in out
    assert 'small.npy' in out


def test_clear_all(temp_cache):
    tempdir = intake.config.confdir
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    md = CacheMetadata()
    assert len(md) == 1
    assert 'states' in list(md)[0]
    subprocess.call('INTAKE_CONF_DIR=%s intake cache clear'
                    '' % tempdir,
                    shell=True)
    md = CacheMetadata()
    assert len(md) == 0


def test_clear_one(temp_cache):
    tempdir = intake.config.confdir
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    cat.arr_cache.read()
    md = CacheMetadata()
    keys = list(md)
    assert len(keys) == 2
    subprocess.call('INTAKE_CONF_DIR=%s intake cache clear %s'
                    '' % (tempdir, keys[0]),
                    shell=True)
    md = CacheMetadata()
    assert len(md) == 1
    assert list(md)[0] == keys[1]


def test_usage(temp_cache):
    tempdir = intake.config.confdir
    from intake.source.cache import BaseCache
    BaseCache(None, None).clear_all()
    out = subprocess.check_output('INTAKE_CONF_DIR=%s INTAKE_CACHE_DIR=%s'
                                  ' intake cache usage' % (tempdir, tempdir),
                                  shell=True).decode()
    assert '0.0' in out  # empty!
