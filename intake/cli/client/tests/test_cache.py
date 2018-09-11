import intake
import intake.config
from intake.source.cache import CacheMetadata
import os
import subprocess
from intake.source.tests.util import temp_cache
cpath = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 '../../../catalog/tests/catalog_caching.yml'))


def test_help(tmpdir):
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache' % tmpdir,
                                  shell=True).decode()
    assert 'usage: ' in out

    out2 = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache -h' % tmpdir,
                                   shell=True).decode()
    assert out2 == out


def test_list_keys(temp_cache):
    tmpdir = intake.config.confdir
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache'
                                  ' list-keys' % tmpdir,
                                  shell=True).decode()
    assert '[]\n' in out  # empty cache
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache list-keys'
                                  '' % tmpdir,
                                  shell=True).decode()
    assert 'states.csv' in out


def test_precache(temp_cache):
    tmpdir = intake.config.confdir
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache list-keys'
                                  '' % tmpdir,
                                  shell=True).decode()
    assert out == "[]\n\n"
    out = subprocess.check_output('INTAKE_CONF_DIR=%s INTAKE_CACHE_DIR=%s '
                                  'intake precache %s ' %
                                  (tmpdir, tmpdir, cpath), shell=True).decode()
    assert out.count('Caching for entry') > 1
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake cache list-keys'
                                  '' % tmpdir,
                                  shell=True).decode()
    assert 'states.csv' in out
    assert 'small.npy' in out


def test_clear_all(temp_cache):
    tmpdir = intake.config.confdir
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    md = CacheMetadata()
    assert len(md) == 1
    assert 'states' in list(md)[0]
    subprocess.call('INTAKE_CONF_DIR=%s intake cache clear'
                    '' % tmpdir,
                    shell=True)
    md = CacheMetadata()
    assert len(md) == 0


def test_clear_one(temp_cache):
    tmpdir = intake.config.confdir
    cat = intake.open_catalog(cpath)
    cat.test_cache.read()
    cat.arr_cache.read()
    md = CacheMetadata()
    keys = list(md)
    assert len(keys) == 2
    subprocess.call('INTAKE_CONF_DIR=%s intake cache clear %s'
                    '' % (tmpdir, keys[0]),
                    shell=True)
    md = CacheMetadata()
    assert len(md) == 1
    assert list(md)[0] == keys[1]


def test_usage(temp_cache):
    tmpdir = intake.config.confdir
    from intake.source.cache import BaseCache
    BaseCache(None, None).clear_all()
    out = subprocess.check_output('INTAKE_CONF_DIR=%s INTAKE_CACHE_DIR=%s'
                                  ' intake cache usage' % (tmpdir, tmpdir),
                                  shell=True).decode()
    assert '0.0' in out  # empty!
