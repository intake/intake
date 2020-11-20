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


def test_par_selector(gui, cat2):
    gui.cat.select.selected = [cat2]
    assert gui.source.pars_widget.disabled is False

    gui.source.pars_widget.value = True
    wid = gui.source.pars_editor.panel[0]
    assert isinstance(wid, pn.widgets.Select)
    assert wid.value == ''
    s = gui.source_instance
    assert s.urlpath.endswith("crime.csv")
    wid.value = "2"
    s = gui.source_instance
    assert s.urlpath.endswith("crime2.csv")


def test_gui_remove_selected_cat(gui, cat1):
    gui.cat.select.remove_selected()
    assert gui.cat.search_widget.disabled is True
    assert gui.source.plot_widget.disabled is True
    assert gui.item is None


def test_gui_open_plot_panel(gui, cat1, cat2, sources1, sources2):
    pytest.importorskip('hvplot')
    gui.source.plot_widget.value = True
    assert gui.source.plot.visible is True
    assert len(gui.source.plot.watchers) == 2
    assert len(gui.source.plot.panel.objects) == 2
    assert gui.source.plot.source.entry == sources1[0]

    gui.source.select.cats = [cat2]
    assert gui.source.plot_widget.value is False  # hide on select change
    assert gui.source.plot.visible is False
    assert not gui.source.plot.watchers

    gui.source.plot_widget.value = True
    assert len(gui.source.plot.watchers) == 2
    assert len(gui.source.plot.panel.objects) == 2


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
    gui.cat.select.selected = [cat2]
    gui.cat.visible = False

    assert gui.cat.select.visible is False
    assert len(gui.cat.control_panel.objects) == 0
    assert gui.cat.search.visible is False
    assert gui.cat.add.visible is False

    gui.cat.visible = True
    assert gui.cat.select.visible is True
    assert len(gui.cat.control_panel.objects) == 3
    assert gui.cat.search.visible is False
    assert gui.cat.add.visible is False
    assert gui.cats == [cat2]


def test_gui_close_and_open_source(gui, cat2, sources2):
    gui.source.visible = False
    gui.cat.select.selected = [cat2]

    assert gui.source.visible is False
    assert len(gui.source.control_panel.objects) == 0
    assert gui.source.description.visible is False
    assert gui.source.plot.visible is False

    gui.source.visible = True
    assert gui.source.select.visible is True
    assert len(gui.source.control_panel.objects) == 2
    assert gui.source.description.visible is True
    assert gui.source.plot.visible is False
    assert gui.source.sources == [sources2[0]]


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


def test_gui_getstate(gui, cat1, sources1):
    state = gui.__getstate__()

    assert state['visible'] is True
    assert state['cat']['visible']is True
    assert state['cat']['add']['visible'] is False
    assert state['cat']['search']['visible'] is False
    assert state['cat']['select']['selected'] == [cat1.name]
    assert state['source']['visible'] is True
    assert state['source']['select']['selected'] == [sources1[0].name]
    assert state['source']['plot']['visible'] is False


def test_gui_state_roundtrip(gui, cat1, cat2, sources1):
    from ..gui import GUI
    other = GUI.from_state(gui.__getstate__())

    assert other.cat.select.items == [cat1, cat2]
    assert other.cats == [cat1]
    assert other.sources == [sources1[0]]
    assert other.cat.search.visible is False
    assert other.cat.add.visible is False
    assert other.source.plot.visible is False
