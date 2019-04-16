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

    assert not gui.cat_adder.watchers
    assert gui.cat_adder.visible is False
    assert gui.cat_add.disabled is False

    assert not gui.searcher.watchers
    assert gui.searcher.visible is False
    assert gui.search.disabled is False

    assert not gui.plotter.watchers
    assert gui.plotter.visible is False
    assert gui.plot.disabled is False


def test_gui_remove_selected_cat(gui, cat1):
    gui.cat_browser.remove_selected()
    assert gui.search.disabled is True
    assert gui.plot.disabled is True
    assert gui.item is None


def test_gui_open_plot_panel(gui, cat1, cat2, sources1, sources2):
    pytest.importorskip('hvplot')
    gui.plot.value = True
    assert gui.plotter.visible is True
    assert len(gui.plotter.watchers) == 3
    assert len(gui.plotter.panel.objects) == 4
    assert gui.plotter.source == sources1[0]

    gui.cat_browser.selected = [cat2]
    assert gui.plot.value is True
    assert gui.plotter.visible is True
    assert len(gui.plotter.watchers) == 3
    assert len(gui.plotter.panel.objects) == 4
    assert gui.plotter.source == sources2[0]

    gui.plot.value = False
    assert not gui.plotter.watchers
    assert gui.plotter.visible is False


def test_gui_open_search_panel(gui, cat1, cat2, sources1, sources2):
    gui.search.value = True
    assert len(gui.searcher.watchers) == 2
    assert len(gui.searcher.panel.objects) == 2
    assert gui.searcher.cats == [cat1]

    gui.cat_browser.selected = [cat2]
    assert len(gui.searcher.watchers) == 2
    assert len(gui.searcher.panel.objects) == 2
    assert gui.searcher.cats == [cat2]

    gui.search.value = False
    assert not gui.searcher.watchers
    assert gui.searcher.visible is False


def test_gui_close_and_open_cat_browser(gui, cat2, sources2):
    assert gui.search.disabled is False

    gui.cat_browser.selected = [cat2]
    assert gui.source_browser.items == sources2
    assert gui.search.disabled is False

    gui.cat_browser.visible = False
    assert gui.source_browser.items == sources2
    assert not gui.cat_browser.watchers
    assert gui.search.disabled is False

    gui.cat_browser.visible = True
    assert len(gui.cat_browser.watchers) == 3
    assert gui.cat_browser.selected == [cat2]
    assert gui.source_browser.items == sources2
    assert gui.search.disabled is False


def test_gui_close_and_open_source_browser(gui, sources1):
    assert gui.source_browser.selected == sources1[:1]
    assert gui.plot.disabled is False

    gui.source_browser.visible = False
    assert not gui.source_browser.watchers
    assert gui.source_browser.selected == sources1[:1]
    assert gui.plot.disabled is False

    gui.source_browser.visible = True
    assert len(gui.source_browser.watchers) == 1
    assert gui.source_browser.selected == sources1[:1]
    assert gui.plot.disabled is False


def test_gui_init_empty():
    from ..gui import GUI
    gui = GUI(cats=[])
    assert gui._cats == []
    assert gui.cats == []
    assert gui.item == None
    assert len(gui.watchers) == 5

    assert not gui.cat_adder.watchers
    assert gui.cat_adder.visible is False
    assert gui.cat_add.disabled is False

    assert not gui.searcher.watchers
    assert gui.searcher.visible is False
    assert gui.search.disabled is True

    assert not gui.plotter.watchers
    assert gui.plotter.visible is False
    assert gui.plot.disabled is True

