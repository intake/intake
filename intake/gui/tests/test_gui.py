#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
pn = pytest.importorskip('panel')


@pytest.fixture
def gui(cat1, cat2):
    from ..gui import GUI
    return GUI(cats=[cat1, cat2])

def test_gui(gui, cat1, cat2, sources1):
    assert gui._cats == [cat1, cat2]
    assert gui.cats == [cat1]
    assert gui.item == sources1[0]
    assert len(gui.watchers) == 5

    assert not gui.cat_add.watchers
    assert gui.cat_add.visible is False
    assert gui.cat_add_toggle.disabled is False

    assert not gui.search.watchers
    assert gui.search.visible is False
    assert gui.search_toggle.disabled is False

    assert not gui.plot.watchers
    assert gui.plot.visible is False
    assert gui.plot_toggle.disabled is False


def test_gui_remove_selected_cat(gui, cat1):
    gui.cat_browser.remove_selected()
    assert gui.search_toggle.disabled is True
    assert gui.plot_toggle.disabled is True
    assert gui.item is None


def test_gui_open_plot_panel(gui, cat1, cat2, sources1, sources2):
    pytest.importorskip('hvplot')
    gui.plot_toggle.value = True
    assert gui.plot.visible is True
    assert len(gui.plot.watchers) == 3
    assert len(gui.plot.panel.objects) == 4
    assert gui.plot.source == sources1[0]

    gui.source_browser.selected = [sources1[1]]
    assert gui.plot_toggle.value is True
    assert gui.plot.visible is True
    assert len(gui.plot.watchers) == 3
    assert len(gui.plot.panel.objects) == 4

    gui.plot_toggle.value = False
    assert not gui.plot.watchers
    assert gui.plot.visible is False


def test_gui_open_search_panel(gui, cat1, cat2, sources1, sources2):
    gui.search_toggle.value = True
    assert len(gui.search.watchers) == 2
    assert len(gui.search.panel.objects) == 2
    assert gui.search.cats == [cat1]

    gui.cat_browser.selected = [cat2]
    assert len(gui.search.watchers) == 2
    assert len(gui.search.panel.objects) == 2
    assert gui.search.cats == [cat2]

    gui.search_toggle.value = False
    assert not gui.search.watchers
    assert gui.search.visible is False


def test_gui_close_and_open_cat_browser(gui, cat2, sources2):
    assert gui.search_toggle.disabled is False

    gui.cat_browser.selected = [cat2]
    assert gui.source_browser.items == sources2
    assert gui.search_toggle.disabled is False

    gui.cat_browser.visible = False
    assert gui.source_browser.items == sources2
    assert not gui.cat_browser.watchers
    assert gui.search_toggle.disabled is False

    gui.cat_browser.visible = True
    assert len(gui.cat_browser.watchers) == 3
    assert gui.cat_browser.selected == [cat2]
    assert gui.source_browser.items == sources2
    assert gui.search_toggle.disabled is False


def test_gui_close_and_open_source_browser(gui, sources1):
    assert gui.source_browser.selected == sources1[:1]
    assert gui.plot_toggle.disabled is False

    gui.source_browser.visible = False
    assert not gui.source_browser.watchers
    assert gui.source_browser.selected == sources1[:1]
    assert gui.plot_toggle.disabled is False

    gui.source_browser.visible = True
    assert len(gui.source_browser.watchers) == 1
    assert gui.source_browser.selected == sources1[:1]
    assert gui.plot_toggle.disabled is False


def test_gui_init_empty():
    from ..gui import GUI
    gui = GUI(cats=[])
    assert gui._cats == []
    assert gui.cats == []
    assert gui.item == None
    assert len(gui.watchers) == 5

    assert not gui.cat_add.watchers
    assert gui.cat_add.visible is False
    assert gui.cat_add_toggle.disabled is False

    assert not gui.search.watchers
    assert gui.search.visible is False
    assert gui.search_toggle.disabled is True

    assert not gui.plot.watchers
    assert gui.plot.visible is False
    assert gui.plot_toggle.disabled is True

