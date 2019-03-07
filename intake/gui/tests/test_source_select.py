import intake
import pytest

pytest.importorskip('panel')


def test_catalog_browser_add(cat_browser, cat1):
    assert cat1.name in cat_browser.options
    assert cat1.name in cat_browser.widget.options
    assert cat1 in cat_browser.selected
    assert cat1 in cat_browser.widget.value


def test_catalog_browser_add_cat_as_str(cat1, cat1_url):

    from ..source_select import CatSelector
    cat_browser = CatSelector()
    cat_browser.add(cat1_url)
    cat = intake.open_catalog(cat1_url)
    assert cat.name in cat_browser.options
    assert cat.name in cat_browser.widget.options
    assert cat in cat_browser.selected
    assert cat in cat_browser.widget.value


def test_catalog_browser_unselect_cat(cat_browser, cat1):
    assert cat1 in cat_browser.selected
    cat_browser.unselect()
    assert cat1 not in cat_browser.selected
    assert cat1 not in cat_browser.widget.value
    assert cat1.name in cat_browser.options
    assert cat1.name in cat_browser.widget.options


def test_catalog_browser_remove_selected_cat(cat_browser, cat1):
    assert cat1 in cat_browser.selected
    cat_browser.remove_selected()
    assert cat1 not in cat_browser.selected
    assert cat1 not in cat_browser.widget.value
    assert cat1.name not in cat_browser.options
    assert cat1.name not in cat_browser.widget.options


def test_catalog_browser_add_another_cat(cat_browser, cat1, cat2):
    assert cat1 in cat_browser.selected
    cat_browser.add(cat2)
    assert cat2 in cat_browser.selected
    assert cat2 in cat_browser.widget.value
    assert cat1 not in cat_browser.selected
    assert cat1 not in cat_browser.widget.value


def test_source_browser_add(source_browser, source1):
    assert source1.name in source_browser.options
    assert source1.name in source_browser.widget.options
    assert source1 in source_browser.selected
    assert source1 in source_browser.widget.value

def test_source_browser_from_cat(cat1, source1):
    from ..source_select import SourceSelector
    source_browser = SourceSelector()
    source_browser.from_cats([cat1, cat2])
    assert source1.name in source_browser.options
    assert source1.name in source_browser.widget.options
    assert source1 in source_browser.selected
    assert source1 in source_browser.widget.value


intake/gui/tests/test_source_select.py::test_catalog_browser PASSED                                 [ 17%]
intake/gui/tests/test_source_select.py::test_catalog_browser_add PASSED                             [ 21%]
intake/gui/tests/test_source_select.py::test_catalog_browser_add_cat_as_str PASSED                  [ 26%]
intake/gui/tests/test_source_select.py::test_catalog_browser_unselect_cat PASSED                    [ 30%]
intake/gui/tests/test_source_select.py::test_catalog_browser_remove_selected_cat PASSED             [ 34%]
intake/gui/tests/test_source_select.py::test_source_browser PASSED                                  [ 39%]
intake/gui/tests/test_source_select.py::test_source_browser_from_cats PASSED                        [ 43%]
intake/gui/tests/test_source_select.py::test_source_browser_add_list PASSED                         [ 47%]
intake/gui/tests/test_source_select.py::test_source_browser_remove PASSED                           [ 52%]
intake/gui/tests/test_source_select.py::test_source_browser_remove_list PASSED                      [ 56%]
intake/gui/tests/test_source_select.py::test_source_browser_select_by_object PASSED                 [ 60%]
intake/gui/tests/test_source_select.py::test_source_browser_select_by_name PASSED                   [ 65%]
intake/gui/tests/test_source_select.py::test_source_browser_unselect PASSED                         [ 69%]
intake/gui/tests/test_source_select.py::test_source_browser_unselect_object PASSED                  [ 73%]
intake/gui/tests/test_source_select.py::test_source_browser_unselect_list_of_objects PASSED         [ 78%]
