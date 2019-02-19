#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import os.path
import pytest

import intake


path = os.path.dirname(__file__)


def test_idempotent(temp_cache):
    pytest.importorskip('zarr')
    cat = intake.open_catalog(os.path.abspath(
        os.path.join(path, '..', '..', 'source', 'tests', 'sources.yaml')))
    s = cat.zarr1()
    assert not s.has_been_persisted
    s2 = s.persist()
    assert s.has_been_persisted
    assert not s.is_persisted
    assert not s2.has_been_persisted
    assert s2.is_persisted
    s3 = s.persist()
    assert s3 == s2

    assert s.get_persisted() == cat.zarr1()
    with pytest.raises(ValueError):
        s2.persist()


def test_parquet(temp_cache):
    inp = pytest.importorskip('intake_parquet')
    cat = intake.open_catalog(os.path.abspath(
        os.path.join(path, 'catalog1.yml')))
    s = cat.entry1()
    s2 = s.persist()
    assert isinstance(s2, inp.ParquetSource)
