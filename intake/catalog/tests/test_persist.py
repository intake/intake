import os
import pytest
import shutil
import time

import intake


@pytest.fixture
def cat():
    path = os.path.dirname(__file__)
    return intake.open_catalog(os.path.join(path, 'catalog1.yml'))


def test_idempotent(cat, temp_cache):
    from intake.container.persist import store
    s = cat.entry1()
    assert not s.has_been_persisted
    s2 = s.persist()
    assert s.has_been_persisted
    assert not s.is_persisted
    assert not s2.has_been_persisted
    assert s2.is_persisted
    s3 = s.persist()
    assert s3 == s2
