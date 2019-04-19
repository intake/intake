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
    assert gui.cat.select.items == [cat1, cat2]
    assert gui.cats == [cat1]
    assert gui.item == sources1[0]

    assert not gui.cat.add.watchers
    assert gui.cat.add.visible is False
    assert gui.cat.add_widget.disabled is False

    assert not gui.cat.search.watchers
    assert gui.cat.search.visible is False
    assert gui.cat.search_widget.disabled is False

    assert not gui.source.plot.watchers
    assert gui.source.plot.visible is False
    assert gui.source.plot_widget.disabled is False


def test_gui_remove_selected_cat(gui, cat1):
    gui.cat.select.remove_selected()
    assert gui.cat.search_widget.disabled is True
    assert gui.source.plot_widget.disabled is True
    assert gui.item is None


def test_gui_open_plot_panel(gui, cat1, cat2, sources1, sources2):
    pytest.importorskip('hvplot')
    gui.source.plot_widget.value = True
    assert gui.source.plot.visible is True
    assert len(gui.source.plot.watchers) == 3
    assert len(gui.source.plot.panel.objects) == 3
    assert gui.source.plot.source == sources1[0]

    gui.source.select.cats = [cat2]
    assert gui.source.plot_widget.value is True
    assert gui.source.plot.visible is True
    assert len(gui.source.plot.watchers) == 3
    assert len(gui.source.plot.panel.objects) == 3

    gui.source.plot_widget.value = False
    assert not gui.source.plot.watchers
    assert gui.source.plot.visible is False


def test_gui_open_search_panel(gui, cat1, cat2, sources1, sources2):
    gui.cat.search_widget.value = True
    assert len(gui.cat.search.watchers) == 2
    assert len(gui.cat.search.panel.objects) == 2
    assert gui.cat.search.cats == [cat1]

    gui.cat.select.selected = [cat2]
    assert len(gui.cat.search.watchers) == 2
    assert len(gui.cat.search.panel.objects) == 2
    assert gui.cat.search.cats == [cat2]

    gui.cat.search_widget.value = False
    assert not gui.cat.search.watchers
    assert gui.cat.search.visible is False


def test_gui_close_and_open_cat(gui, cat2, sources2):
    assert gui.cat.search_widget.disabled is False

    gui.cat.select.selected = [cat2]
    assert gui.source.select.items == sources2
    assert gui.cat.search_widget.disabled is False

    gui.cat.select.visible = False
    assert gui.source.select.items == sources2
    assert not gui.cat.select.watchers
    assert gui.cat.search_widget.disabled is False

    gui.cat.select.visible = True
    assert len(gui.cat.select.watchers) == 1
    assert gui.cat.select.selected == [cat2]
    assert gui.source.select.items == sources2
    assert gui.cat.search_widget.disabled is False


def test_gui_close_and_open_source_select(gui, sources1):
    assert gui.source.select.selected == sources1[:1]
    assert gui.source.plot_widget.disabled is False

    gui.source.select.visible = False
    assert not gui.source.select.watchers
    assert gui.source.select.selected == sources1[:1]
    assert gui.source.plot_widget.disabled is False

    gui.source.select.visible = True
    assert len(gui.source.select.watchers) == 1
    assert gui.source.select.selected == sources1[:1]
    assert gui.source.plot_widget.disabled is False


def test_gui_init_empty():
    from ..gui import GUI
    gui = GUI(cats=[])
    assert gui.cat.select.items == []
    assert gui.cats == []
    assert gui.item == None

    assert not gui.cat.add.watchers
    assert gui.cat.add.visible is False
    assert gui.cat.add_widget.disabled is False

    assert not gui.cat.search.watchers
    assert gui.cat.search.visible is False
    assert gui.cat.search_widget.disabled is True

    assert not gui.source.plot.watchers
    assert gui.source.plot.visible is False
    assert gui.source.plot_widget.disabled is True

