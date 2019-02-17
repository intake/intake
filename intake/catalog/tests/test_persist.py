import os
import posixpath
import pytest

import intake


@pytest.fixture
def cat():
    path = os.path.dirname(__file__)
    return intake.open_catalog(posixpath.abspath(
        posixpath.join(path, '..', '..', 'source', 'tests', 'sources.yaml')))


def test_idempotent(cat, temp_cache):
    s = cat.zarr1()
    assert not s.has_been_persisted
    s2 = s.persist()
    assert s.has_been_persisted
    assert not s.is_persisted
    assert not s2.has_been_persisted
    assert s2.is_persisted
    s3 = s.persist()
    assert s3 == s2
