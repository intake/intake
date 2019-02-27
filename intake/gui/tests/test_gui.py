#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
import intake

here = os.path.abspath(os.path.dirname(__file__))
cat1_url = os.path.join(here, '..', '..', 'catalog', 'tests', 'catalog1.yml')
cat2_url = os.path.join(here, '..', '..', 'catalog', 'tests', 'catalog_union_2.yml')


def test_add_cat():
    pytest.importorskip('ipywidgets')
    intake.gui.add_cat(os.path.join(here, '..', '..', 'catalog', 'tests',
                                    'catalog1.yml'))
    assert 'catalog1' in intake.gui.cat_list.value
    assert 'entry1' in intake.gui.item_list.options


@pytest.fixture
def cat1():
    return intake.open_catalog(cat1_url)

@pytest.fixture
def cat2():
    return intake.open_catalog(cat2_url)

@pytest.fixture
def gui(cat1):
    from intake.gui import CatalogBrowser
    gui = CatalogBrowser()
    gui.add(cat1)
    return gui


def test_catalog_browser_add_cat_as_cat(gui, cat1):
    pytest.importorskip('panel')
    assert cat1.name in gui.options
    assert cat1.name in gui.widget.options
    assert cat1 in gui.selected
    assert cat1 in gui.widget.value


def test_catalog_browser_add_cat_as_str(cat1):
    pytest.importorskip('panel')
    import intake
    from intake.gui import CatalogBrowser
    gui = CatalogBrowser()
    gui.add(cat1_url)
    cat = intake.open_catalog(cat1_url)
    assert cat.name in gui.options
    assert cat.name in gui.widget.options
    assert cat in gui.selected
    assert cat in gui.widget.value


def test_catalog_browser_unselect_cat(gui, cat1):
    pytest.importorskip('panel')
    assert cat1 in gui.selected
    gui.unselect()
    assert cat1 not in gui.selected
    assert cat1 not in gui.widget.value
    assert cat1.name in gui.options
    assert cat1.name in gui.widget.options


def test_catalog_browser_del_cat(gui, cat1):
    pytest.importorskip('panel')
    assert cat1 in gui.selected
    gui.del_selected()
    assert cat1 not in gui.selected
    assert cat1 not in gui.widget.value
    assert cat1.name not in gui.options
    assert cat1.name not in gui.widget.options

def test_catalog_browser_add_another_cat(gui, cat1, cat2):
    pytest.importorskip('panel')
    assert cat1 in gui.selected
    gui.add(cat2)
    assert cat2 in gui.selected
    assert cat2 in gui.widget.value
    assert cat1 not in gui.selected
    assert cat1 not in gui.widget.value
