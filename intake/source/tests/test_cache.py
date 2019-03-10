#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
import shutil

from intake.source.cache import FileCache, CacheMetadata
import intake
import intake.config
import logging
here = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger('intake')


@pytest.fixture
def file_cache():
    return FileCache(
        'csv', {'argkey': 'urlpath', 'regex': 'test/path', 'type': 'file'})


def test_ensure_cache_dir(file_cache):
    file_cache._ensure_cache_dir()
    assert os.path.exists(file_cache._cache_dir)

    file_cache.clear_all()
    shutil.rmtree(file_cache._cache_dir)

    with open(file_cache._cache_dir, 'w') as f:
        f.write('')

    with pytest.raises(Exception):
        file_cache._ensure_cache_dir()

    os.remove(file_cache._cache_dir)

    file_cache.clear_all()


def test_munge_path(file_cache):
    subdir = 'subdir'
    cache_path = file_cache._munge_path(subdir, 'test/path/foo.cvs')
    assert subdir in cache_path
    assert 'test/path' not in cache_path

    file_cache._spec['regex'] = 'https://example.com'
    cache_path = file_cache._munge_path(subdir, 'https://example.com/catalog.yml')
    assert subdir in cache_path
    assert file_cache._cache_dir in cache_path
    assert 'http' not in cache_path


def test_hash(file_cache):
    subdir = file_cache._hash('foo/bar.csv')

    import string
    # Checking for md5 hash
    assert all(c in string.hexdigits for c in subdir)

    file_cache._driver = 'bar'
    subdir_new = file_cache._hash('foo/bar.csv')
    assert subdir_new != subdir

    file_cache._driver = 'csv'
    subdir_new = file_cache._hash('foo/bar.csv')
    assert subdir_new == subdir

    file_cache._spec['regex'] = 'foo/bar'
    subdir_new = file_cache._hash('foo/bar.csv')
    assert subdir_new != subdir


def test_path(file_cache):
    urlpath = 'test/path/foo.csv'
    file_cache._spec['regex'] = 'test/path/'
    cache_path = file_cache._path(urlpath)

    assert file_cache._cache_dir in cache_path
    assert '//' not in cache_path[1:]
    file_cache.clear_all()


def test_dir_cache(tempdir, temp_cache):
    [os.makedirs(os.path.join(tempdir, d)) for d in [
        'main', 'main/sub1', 'main/sub2']]
    for f in ['main/afile', 'main/sub1/subfile', 'main/sub2/subfile1',
              'main/sub2/subfile2']:
        fn = os.path.join(tempdir, f)
        with open(fn, 'w') as fo:
            fo.write(f)
    fn = os.path.join(tempdir, 'cached.yaml')
    shutil.copy2(os.path.join(here, 'cached.yaml'), fn)
    cat = intake.open_catalog(fn)
    s = cat.dirs()
    out = s.cache[0].load(s._urlpath, output=False)
    assert out[0] == os.path.join(tempdir, s.cache[0]._path(s._urlpath))
    assert open(os.path.join(out[0], 'afile')).read() == 'main/afile'
    md = CacheMetadata()
    got = md[s._urlpath]

    # Avoid re-copy
    s = cat.dirs()
    s.cache[0].load(s._urlpath, output=False)
    md2 = CacheMetadata()
    got2 = md2[s._urlpath]
    assert got == got2


def test_compressed_cache(temp_cache):
    cat = intake.open_catalog(os.path.join(here, 'cached.yaml'))
    s = cat.calvert()
    old = intake.config.conf['cache_download_progress']
    try:
        intake.config.conf['cache_download_progress'] = False
        df = s.read()
        assert len(df)
        md = CacheMetadata()
        assert len(md[s._urlpath]) == 1  # we gained exactly one CSV
        intake.config.conf['cache_download_progress'] = False
        df = s.read()
        assert len(df)
        md = CacheMetadata()
        assert len(md[s._urlpath]) == 1  # we still have exactly one CSV
    finally:
        intake.config.conf['cache_download_progress'] = old


def test_filtered_compressed_cache(temp_cache):
    cat = intake.open_catalog(os.path.join(here, 'cached.yaml'))
    s = cat.calvert_filter()
    old, intake.config.conf['cache_download_progress'] = intake.config.conf['cache_download_progress'], False
    try:
        df = s.read()
        assert len(df)
        md = CacheMetadata()
        assert len(md[s._urlpath]) == 1  # we gained exactly one CSV
        intake.config.conf['cache_download_progress'] = False
        df = s.read()
        assert len(df)
        md = CacheMetadata()
        assert len(md[s._urlpath]) == 1  # we still have exactly one CSV
    finally:
        intake.config.conf['cache_download_progress'] = old


def test_cache_to_cat(tempdir):
    old = intake.config.conf.copy()
    olddir = intake.config.confdir
    intake.config.confdir = str(tempdir)
    intake.config.conf.update({'cache_dir': 'catdir',
                               'cache_download_progress': False,
                               'cache_disabled': False})
    try:
        fn0 = os.path.join(here, 'calvert_uk.zip')
        fn1 = os.path.join(tempdir, 'calvert_uk.zip')
        shutil.copy2(fn0, fn1)
        fn0 = os.path.join(here, 'cached.yaml')
        fn1 = os.path.join(tempdir, 'cached.yaml')
        shutil.copy2(fn0, fn1)
        cat = intake.open_catalog(fn1)
        s = cat.calvert()
        df = s.read()
        assert len(df)
        md = CacheMetadata()
        f = md[s._urlpath][0]
        assert f['cache_path'].startswith(str(tempdir))
        assert 'intake_cache' in os.listdir(tempdir)
        assert os.listdir(os.path.join(tempdir, 'intake_cache'))
    finally:
        intake.config.confdir = olddir
        intake.config.conf.update(old)


def test_compressed_cache_infer(temp_cache):
    cat = intake.open_catalog(os.path.join(here, 'cached.yaml'))
    s = cat.calvert_infer()
    old = intake.config.conf['cache_download_progress']
    try:
        intake.config.conf['cache_download_progress'] = False
        df = s.read()
        assert len(df)
    finally:
        intake.config.conf['cache_download_progress'] = old


@pytest.mark.skipif(os.name == 'nt', reason="No CLI tools on windows")
@pytest.mark.parametrize('comp', ['tgz', 'tbz', 'tar', 'gz', 'bz'])
def test_compressions(temp_cache, tempdir, comp):
    from intake.source.cache import CompressedCache
    import shlex
    import subprocess
    data = b'hello'
    fn = os.path.join(tempdir, 'data')
    with open(fn, 'wb') as f:
        f.write(data)
    if comp == 'tgz':
        subprocess.call(shlex.split('tar -czf {fn}.tgz {fn} -C {d}'.format(
            fn=fn, d=tempdir)))
    elif comp == 'tbz':
        subprocess.call(shlex.split('tar -cjf {fn}.tbz {fn} -C {d}'.format(
            fn=fn, d=tempdir)))
    elif comp == 'tar':
        subprocess.call(shlex.split('tar -cf {fn}.tar {fn} -C {d}'.format(
            fn=fn, d=tempdir)))
    elif comp == 'gz':
        subprocess.call(shlex.split('gzip ' + fn))
    elif comp == 'bz':
        subprocess.call(shlex.split('bzip2 ' + fn))
    fn = os.path.join(
        tempdir,
        [f for f in os.listdir(tempdir) if f.startswith('data.')][0])
    comp = CompressedCache(driver=None, spec={'decomp': comp})
    comp.output = None
    files_in, files_out = comp._make_files(fn)
    out = comp._load(files_in, files_out, fn, meta=False)
    with open(out[0], 'rb') as f:
        assert f.read() == b'hello'

    comp = CompressedCache(driver=None, spec={'decomp': 'infer'})
    comp.output = None
    files_in, files_out = comp._make_files(fn)
    out = comp._load(files_in, files_out, fn, meta=False)
    with open(out[0], 'rb') as f:
        assert f.read() == b'hello'


def test_compressed_cache_bad(temp_cache):
    cat = intake.open_catalog(os.path.join(here, 'cached.yaml'))
    s = cat.calvert_badkey()
    old = intake.config.conf['cache_download_progress']
    try:
        intake.config.conf['cache_download_progress'] = False
        with pytest.raises(ValueError):
            s.read()
    finally:
        intake.config.conf['cache_download_progress'] = old


@pytest.mark.xfail
def test_dat(temp_cache):
    import subprocess
    import time
    try:
        subprocess.call(['dat', '-v'])
    except Exception:
        pytest.skip("DAT not avaiable")
    P = subprocess.Popen(['dat', 'share', '-d',
                          os.path.join(here, 'test_dat/')])
    time.sleep(1.5)
    try:
        cat = intake.open_catalog(os.path.join(here, 'cached.yaml'))
        out = cat.dat_data.read()
        assert out[0]['title'] == 'intake test copy'
    finally:
        P.terminate()
