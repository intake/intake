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


def test_source_browser_init_with_cats(cat1, cat2, sources1, sources2):
    from ..select import SourceSelector
    source_browser = SourceSelector(cats=[cat1, cat2])
    assert sources1[0].name in source_browser.options
    assert sources2[0].name in source_browser.options
    assert sources1[0] in source_browser.selected
    assert_widget_matches(source_browser)


def test_source_browser_set_cats(cat1, cat2, sources1, sources2):
    from ..select import SourceSelector
    source_browser = SourceSelector()
    source_browser.cats = [cat1, cat2]
    assert sources1[0].name in source_browser.options
    assert sources2[0].name in source_browser.options
    assert sources1[0] in source_browser.selected
    assert_widget_matches(source_browser)


def test_source_browser(source_browser, cat1, sources1):
    assert len(source_browser.cats) == 1
    assert cat1 in source_browser.cats
    for source in sources1:
        assert source.name in source_browser.options
    assert source_browser.selected == [sources1[0]]
    assert_widget_matches(source_browser)


def test_source_browser_add(source_browser, sources1, sources2):
    source_browser.add(sources2[0])
    for source in sources1:
        assert source.name in source_browser.options
    assert sources2[0].name in source_browser.options
    assert source_browser.selected == [sources2[0]]
    assert_widget_matches(source_browser)


def test_source_browser_add_list(source_browser, sources2):
    source_browser.add(sources2)
    assert sources2[1].name in source_browser.options
    assert source_browser.selected == [sources2[0]]
    assert_widget_matches(source_browser)


def test_source_browser_add_entry_with_nonunique_name(source_browser):
    from intake.catalog.local import LocalCatalogEntry
    name = source_browser.labels[0]
    e0 = LocalCatalogEntry(name, '', 'csv', args=dict(urlpath='foo'))
    source_browser.add(e0)
    assert f'{name}_0' in source_browser.labels
    assert name in source_browser.labels
    assert_widget_matches(source_browser)

    e1 = LocalCatalogEntry(name, '', 'csv', args=dict(urlpath='foo1'))
    source_browser.add(e1)
    assert f'{name}_1' in source_browser.labels
    assert f'{name}_0' in source_browser.labels
    assert name in source_browser.labels
    assert_widget_matches(source_browser)


def test_source_browser_remove(source_browser, sources1):
    source_browser.remove(sources1[0])
    assert sources1[0].name not in source_browser.options
    assert source_browser.selected == []
    assert_widget_matches(source_browser)


def test_source_browser_remove_list(source_browser, sources1):
    source_browser.remove(sources1)
    assert source_browser.options == {}
    assert source_browser.selected == []
    assert_widget_matches(source_browser)


def test_source_browser_select_object(source_browser, sources1):
    source_browser.selected = sources1[1]
    assert source_browser.selected == [sources1[1]]
    assert_widget_matches(source_browser)


def test_source_browser_select_name(source_browser, sources1):
    source_browser.selected = sources1[1].name
    assert source_browser.selected == [sources1[1]]
    assert_widget_matches(source_browser)


def test_source_browser_select_list_of_names(source_browser, sources1):
    source_browser.selected = []
    source_browser.selected = [source.name for source in sources1]
    assert source_browser.selected == sources1
    assert_widget_matches(source_browser)


def test_source_browser_select_list_of_objects(source_browser, sources1):
    source_browser.selected = sources1
    assert source_browser.selected == sources1
    assert_widget_matches(source_browser)
