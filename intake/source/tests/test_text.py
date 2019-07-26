#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
import intake
from intake.source.textfiles import TextFilesSource
from intake.source import import_name
from fsspec import open_files
here = os.path.abspath(os.path.dirname(__file__))


def test_textfiles(tempdir):
    open(os.path.join(tempdir, '1.txt'), 'wt').write('hello\nworld')
    open(os.path.join(tempdir, '2.txt'), 'wt').write('hello\nworld')
    path = os.path.join(tempdir, '*.txt')
    t = TextFilesSource(path)
    t.discover()
    assert t.npartitions == 2
    assert t._get_partition(0) == t.to_dask().to_delayed()[0].compute()
    out = t.read()
    assert isinstance(out, list)
    assert out[0] == 'hello\n'


@pytest.mark.parametrize('comp', [None, 'gzip', 'bz2'])
def test_complex_text(tempdir, comp):
    dump, load, read = 'json.dumps', 'json.loads', True
    dump = import_name(dump)
    data = [{'something': 'simple', 'and': 0}] * 2
    for f in ['1.out', '2.out']:
        fn = os.path.join(tempdir, f)
        with open_files([fn], mode='wt', compression=comp)[0] as fo:
            if read:
                fo.write(dump(data))
            else:
                dump(data, fo)
    # that was all setup

    path = os.path.join(tempdir, '*.out')
    t = TextFilesSource(path, text_mode=True, compression=comp,
                        decoder=load)
    t.discover()
    assert t.npartitions == 2
    assert t._get_partition(0) == t.to_dask().to_delayed()[0].compute()
    out = t.read()
    assert isinstance(out, list)
    assert out[0] == data[0]


@pytest.mark.parametrize('comp', [None, 'gzip', 'bz2'])
@pytest.mark.parametrize('pars', [['msgpack.pack', 'msgpack.unpack', False],
                                  ['msgpack.packb', 'msgpack.unpackb', True],
                                  ['pickle.dump', 'pickle.load', False],
                                  ['pickle.dumps', 'pickle.loads', True]])
def test_complex_bytes(tempdir, comp, pars):
    dump, load, read = pars
    dump = import_name(dump)
    # using bytestrings means not needing extra en/decode argument to msgpack
    data = [{b'something': b'simple', b'and': 0}] * 2
    for f in ['1.out', '2.out']:
        fn = os.path.join(tempdir, f)
        with open_files([fn], mode='wb', compression=comp)[0] as fo:
            if read:
                fo.write(dump(data))
            else:
                dump(data, fo)
    # that was all setup

    path = os.path.join(tempdir, '*.out')
    t = TextFilesSource(path, text_mode=False, compression=comp,
                        decoder=load, read=read)
    t.discover()
    assert t.npartitions == 2
    assert t._get_partition(0) == t.to_dask().to_delayed()[0].compute()
    out = t.read()
    assert isinstance(out, list)
    assert out[0] == data[0]


def test_text_persist(temp_cache):
    cat = intake.open_catalog(os.path.join(here, 'sources.yaml'))
    s = cat.sometext()
    s2 = s.persist()
    assert s.read() == s2.read()


def test_text_export(temp_cache):
    import tempfile
    outdir = tempfile.mkdtemp()
    cat = intake.open_catalog(os.path.join(here, 'sources.yaml'))
    s = cat.sometext()
    out = s.export(outdir)
    fn = os.path.join(outdir, 'cat.yaml')
    with open(fn, 'w') as f:
        f.write(out.yaml())
    cat = intake.open_catalog(fn)
    s2 = cat[s.name]()
    assert s.read() == s2.read()
