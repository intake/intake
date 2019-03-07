import intake
import pytest

pytest.importorskip('panel')

def assert_widget_matches(browser):
    assert browser.options == browser.widget.options
    assert browser.selected == browser.widget.value


def test_catalog_browser(cat_browser, cat1):
    assert cat1.name in cat_browser.options
    assert cat_browser.selected == [cat1]
    assert_widget_matches(cat_browser)


def test_catalog_browser_add(cat_browser, cat2):
    cat_browser.add(cat2)
    assert cat2.name in cat_browser.options
    assert cat_browser.selected == [cat2]
    assert_widget_matches(cat_browser)


def test_catalog_browser_add_list(cat_browser, cat2):
    cat_browser.add([cat2])
    assert cat2.name in cat_browser.options
    assert cat_browser.selected == [cat2]
    assert_widget_matches(cat_browser)


def test_catalog_browser_add_cat_as_str(cat_browser, cat2, cat2_url):
    cat_browser.add(cat2_url)
    assert cat2.name in cat_browser.options
    assert cat_browser.selected == [cat2]
    assert_widget_matches(cat_browser)


def test_catalog_browser_unselect_cat(cat_browser, cat1):
    cat_browser.unselect()
    assert cat1.name in cat_browser.options
    assert cat_browser.selected == []
    assert_widget_matches(cat_browser)


def test_catalog_browser_remove_selected_cat(cat_browser):
    cat_browser.remove_selected()
    assert cat_browser.options == []
    assert cat_browser.selected == []
    assert_widget_matches(cat_browser)


def test_source_browser_from_cats(cat1, cat2):
    from ..source_select import SourceSelector
    source_browser = SourceSelector()
    source_browser.from_cats([cat1, cat2])
    assert sources1[0].name in source_browser.options
    assert sources1[0] in source_browser.selected
    assert_widget_matches(source_browser)


def test_source_browser(source_browser, sources1):
    assert sources1[0].name in source_browser.options
    assert source_browser.selected == sources1
    assert_widget_matches(source_browser)

def test_source_browser_add(source_browser, sources2):
    source_browser.add(sources2[0])
    assert sources2[0].name in source_browser.options
    assert source_browser.selected == [sources2[0]]
    assert_widget_matches(source_browser)

def test_source_browser_add_list(source_browser, sources2):
    source_browser.add(sources2)
    assert sources2[1].name in source_browser.options
    assert source_browser.selected == sources2
    assert_widget_matches(source_browser)

def test_source_browser_remove(source_browser, sources1):
    source_browser.remove(sources1[0])
    assert sources1[0].name not in source_browser.options
    assert source_browser.selected == [sources1[1:]]
    assert_widget_matches(source_browser)

def test_source_browser_remove_list(source_browser, sources1):
    source_browser.add(sources1)
    assert source_browser.options == []
    assert source_browser.selected == []
    assert_widget_matches(source_browser)

def test_source_browser_select_by_object(source_browser, sources1):
    source_browser.select(sources1[1])
    assert source_browser.selected == [sources1[1]]
    assert_widget_matches(source_browser)

def test_source_browser_select_by_name(source_browser, sources1):
    source_browser.select(sources1[1].name)
    assert source_browser.selected == [sources1[1]]
    assert_widget_matches(source_browser)

def test_source_browser_unselect(source_browser, sources1):
    source_browser.unselect()
    assert source_browser.selected == []
    assert list(source_browser.options.values()) == sources1
    assert_widget_matches(source_browser)

def test_source_browser_unselect_object(source_browser, sources1):
    source_browser.unselect(sources1[0])
    assert source_browser.selected == [sources1[1:]]
    assert list(source_browser.options.values()) == sources1
    assert_widget_matches(source_browser)

def test_source_browser_unselect_list_of_objects(source_browser, sources1):
    source_browser.unselect(sources1)
    assert source_browser.selected == []
    assert list(source_browser.options.values()) == sources1
    assert_widget_matches(source_browser)
