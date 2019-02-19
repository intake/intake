import os

from intake.container.persist import store
from intake.source.base import DataSource


def test_store(temp_cache):
    assert list(store) == []
    s = DataSource(metadata={'original_name': 'blah'})
    store.add(s._tok, s)
    store.ttl = 0
    assert list(store) == [s._tok]
    assert store.get_tok(s) == s._tok
    assert store.needs_refresh(s) is False  # because it has no TTL
    store.remove(s)
    assert list(store) == []
    assert os.path.exists(store.pdir)
    store.clear()
    assert not os.path.exists(store.pdir)
    assert list(store) == []
