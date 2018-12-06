#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import glob
import os
from intake.source.textfiles import TextFilesSource


def test_textfiles(tempdir):
    open(os.path.join(tempdir, '1.txt'), 'wt').write('hello\nworld')
    open(os.path.join(tempdir, '2.txt'), 'wt').write('hello\nworld')
    path = os.path.join(tempdir, '*.txt')
    t = TextFilesSource(path)
    t.discover()
    assert t.npartitions == len(glob.glob(path))
    assert t._get_partition(0) == t.to_dask().to_delayed()[0].compute()
    out = t.read()
    assert isinstance(out, list)
    assert out[0] == 'hello\n'
