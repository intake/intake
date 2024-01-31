import pytest

from intake.readers.utils import LazyDict, PartlyLazyDict


class OnlyOkeKey(LazyDict):
    def __getitem__(self, item):
        if item == 5:
            return 5
        raise KeyError

    def __iter__(self):
        return iter(range(10))


def test_lazy_dict():
    ld = OnlyOkeKey()
    assert list(ld) == list(range(10))
    assert 5 in ld
    assert ld[5] == 5
    with pytest.raises(KeyError):
        ld[4]

    pld = PartlyLazyDict({12: 2}, ld)
    assert set(pld) == {12} | set(range(10))
    assert 12 in pld
    assert 5 in pld
    assert 0 in pld
    assert pld[12] == 2
    assert pld[5] == 5
    with pytest.raises(KeyError):
        pld[0]
