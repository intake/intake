import intake
import pytest


def test_catalog_browser_add(cat_browser, cat1):
    pytest.importorskip('panel')
    assert cat1.name in cat_browser.options
    assert cat1.name in cat_browser.widget.options
    assert cat1 in cat_browser.selected
    assert cat1 in cat_browser.widget.value


def test_catalog_browser_add_cat_as_str(cat1, cat1_url):
    pytest.importorskip('panel')
    from ..source_select import CatSelector
    cat_browser = CatSelector()
    cat_browser.add(cat1_url)
    cat = intake.open_catalog(cat1_url)
    assert cat.name in cat_browser.options
    assert cat.name in cat_browser.widget.options
    assert cat in cat_browser.selected
    assert cat in cat_browser.widget.value


def test_catalog_browser_unselect_cat(cat_browser, cat1):
    pytest.importorskip('panel')
    assert cat1 in cat_browser.selected
    cat_browser.unselect()
    assert cat1 not in cat_browser.selected
    assert cat1 not in cat_browser.widget.value
    assert cat1.name in cat_browser.options
    assert cat1.name in cat_browser.widget.options


def test_catalog_browser_remove_selected_cat(cat_browser, cat1):
    pytest.importorskip('panel')
    assert cat1 in cat_browser.selected
    cat_browser.remove_selected()
    assert cat1 not in cat_browser.selected
    assert cat1 not in cat_browser.widget.value
    assert cat1.name not in cat_browser.options
    assert cat1.name not in cat_browser.widget.options


def test_catalog_browser_add_another_cat(cat_browser, cat1, cat2):
    pytest.importorskip('panel')
    assert cat1 in cat_browser.selected
    cat_browser.add(cat2)
    assert cat2 in cat_browser.selected
    assert cat2 in cat_browser.widget.value
    assert cat1 not in cat_browser.selected
    assert cat1 not in cat_browser.widget.value


def test_source_browser_add(source_browser, source1):
    pytest.importorskip('panel')
    assert source1.name in source_browser.options
    assert source1.name in source_browser.widget.options
    assert source1 in source_browser.selected
    assert source1 in source_browser.widget.value

def test_source_browser_from_cat(cat1, source1):
    pytest.importorskip('panel')
    from ..source_select import SourceSelector
    source_browser = SourceSelector()
    source_browser.from_cats([cat1, cat2])
    assert source1.name in source_browser.options
    assert source1.name in source_browser.widget.options
    assert source1 in source_browser.selected
    assert source1 in source_browser.widget.value
