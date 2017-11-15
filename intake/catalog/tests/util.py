def assert_items_equal(a, b):
    assert len(a) == len(b) and sorted(a) == sorted(b)
