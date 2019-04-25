#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import intake
import pytest

pytest.importorskip('panel')


def assert_widget_matches(browser):
    assert browser.options == browser.widget.options
    assert browser.selected == browser.widget.value


def test_catalog_browser_init_emtpy():
    from ..select import CatSelector
    cat_browser = CatSelector()
    assert cat_browser.selected == [intake.cat]
    assert_widget_matches(cat_browser)


def test_catalog_browser(cat_browser, cat1):
    assert cat_browser.items == [cat1]
    assert cat1.name in cat_browser.options
    assert cat_browser.selected == [cat1]
    assert_widget_matches(cat_browser)


def test_catalog_browser_set_to_visible_and_back(cat_browser, cat1):
    cat_browser.visible = False
    assert len(cat_browser.watchers) == 0

    cat_browser.visible = True
    assert len(cat_browser.watchers) == 1
    assert cat_browser.items == [cat1]
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


def test_catalog_browser_add_nested_catalog(cat_browser, parent_cat):
    cat_browser.add(parent_cat)
    assert parent_cat.name in cat_browser.options
    assert cat_browser.selected == [parent_cat]
    assert list(cat_browser.options.keys()) == ['catalog1', 'parent', '└── child1', '└── child2']
    assert_widget_matches(cat_browser)


def test_catalog_browser_select_cat_by_widget(cat_browser, cat1):
    cat_browser.selected = []
    assert cat_browser.selected == []
    assert_widget_matches(cat_browser)

    cat_browser.widget.value = [cat1]
    assert cat_browser.selected == [cat1]
    assert_widget_matches(cat_browser)


def test_catalog_browser_remove_selected_cat(cat_browser, cat1):
    cat_browser.remove_selected()
    assert cat1 not in cat_browser.options
    assert cat_browser.selected == []
    assert_widget_matches(cat_browser)


def test_catalog_browser_remove_cat_that_is_not_in_options_passes(cat_browser, cat2):
    assert cat2.name not in cat_browser.options
    cat_browser.remove(cat2)


def test_catalog_browser_remove_nested_catalog(cat_browser, parent_cat):
    cat_browser.add(parent_cat)
    assert parent_cat.name in cat_browser.options
    assert cat_browser.selected == [parent_cat]
    assert list(cat_browser.options.keys()) == ['catalog1', 'parent', '└── child1', '└── child2']
    cat_browser.remove_selected()
    assert list(cat_browser.options.keys()) == ['catalog1']
    assert_widget_matches(cat_browser)
