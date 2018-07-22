import glob
import os
from intake.source.textfiles import TextFilesSource


def test_textfiles(tmpdir):
    open(os.path.join(tmpdir, '1.txt'), 'wt').write('hello\nworld')
    open(os.path.join(tmpdir, '2.txt'), 'wt').write('hello\nworld')
    path = os.path.join(tmpdir, '*.txt')
    t = TextFilesSource(path)
    t.discover()
    assert t.npartitions == len(glob.glob(path))
    assert t._get_partition(0) == t.to_dask().to_delayed()[0].compute()
    out = t.read()
    assert isinstance(out, list)
    assert out[0] == 'hello\n'
