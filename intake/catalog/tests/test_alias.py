import intake
import os

here = os.path.abspath(os.path.dirname(__file__))
fn = os.path.join(here, 'catalog_alias.yml')


def test_simple():
    cat = intake.open_catalog(fn)
    s = cat.alias0()
    assert s.container == 'other'
    out = str(s.discover())
    assert s.container == 'dataframe'
    assert "state" in out


def test_mapping():
    cat = intake.open_catalog(fn)
    s = cat.alias1()
    assert s.container == 'other'
    out = str(s.discover())
    assert s.container == 'dataframe'
    assert "state" in out

    s = cat.alias1(choice='second')
    assert s.container == 'other'
    out = str(s.discover())
    assert s.container == 'ndarray'
    assert "int64" in out
