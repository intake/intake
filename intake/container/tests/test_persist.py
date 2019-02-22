import os
import time

from intake.container.persist import store
from intake.source.textfiles import TextFilesSource


def test_store(temp_cache):
    assert list(store) == []
    s = DataSource(metadata={'original_name': 'blah'})
    store.add(s._tok, s)
    time.sleep(0.2)

    store.ttl = 0
    assert list(store) == [s._tok]
    assert store.get_tok(s) == s._tok
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
