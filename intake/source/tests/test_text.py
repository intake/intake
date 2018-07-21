import glob
from intake.source.textfiles import TextFilesSource


def test_textfiles():
    t = TextFilesSource('./*.py')
    t.discover()
    assert t.npartitions == len(glob.glob('./*.py'))
    assert t._get_partition(0) == t.to_dask().to_delayed()[0].compute()
    out = t.read()
    assert isinstance(out, list)
    assert isinstance(out[0], str)
