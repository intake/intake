#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
import time

from intake.container.persist import store
from intake.source.textfiles import TextFilesSource
from intake.source.base import DataSource


def test_store(temp_cache):
    from dask.base import tokenize
    assert list(store) == []
    s = DataSource(metadata={'original_name': 'blah'})
    token = tokenize(s)
    store.add(token, s)
    time.sleep(0.2)

    store.ttl = 0
    assert list(store) == [token]
    assert store.get_tok(s) == token
    assert store.needs_refresh(s) is False  # because it has no TTL

    store.remove(s)
    time.sleep(0.2)

    assert list(store) == []
    assert os.path.exists(store.pdir)
    store.clear()
    time.sleep(0.2)

    assert not os.path.exists(store.pdir)
    assert list(store) == []


def test_backtrack(temp_cache):
    s = TextFilesSource("*.py")
    s2 = s.persist()
    s3 = store.backtrack(s2)
    assert s3 == s


def test_persist_with_nonnumeric_ttl_raises_error(temp_cache):
    s = TextFilesSource("*.py")
    with pytest.raises(ValueError, match="User-provided ttl was a string"):
        s.persist(ttl='a string')


class DummyDataframe(DataSource):
    name = 'dummy'
    container = 'dataframe'

    def __init__(self, *args):
        DataSource.__init__(self)

    def read(self):
        import pandas as pd
        return pd.DataFrame({'a': [0]})


def test_undask_persist(temp_cache):
    pytest.importorskip('intake_parquet')
    s = DummyDataframe()
    s2 = s.persist()
    assert s.read().to_dict() == s2.read().to_dict()
