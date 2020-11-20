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
    from ..gui import CatGUI
    return CatGUI(cats=[cat1, cat2])


def test_gui(gui, cat1, cat2):
    assert gui.select.items == [cat1, cat2]
    assert gui.cats == [cat1]

    assert not gui.add.watchers
    assert gui.add.visible is False
    assert gui.add_widget.disabled is False

    assert not gui.search.watchers
    assert gui.search.visible is False
    assert gui.search_widget.disabled is False


def test_gui_close_and_open_select(gui, cat2, sources2):
    gui.select.selected = [cat2]
    gui.select.visible = False
    assert not gui.select.watchers
    assert gui.search_widget.disabled is False

    gui.select.visible = True
    assert len(gui.select.watchers) == 1
    assert gui.select.selected == [cat2]
    assert gui.search_widget.disabled is False


def test_gui_getstate(gui, cat1, sources1):
    state = gui.__getstate__()

    assert state['visible']is True
    assert state['add']['visible'] is False
    assert state['search']['visible'] is False
    assert state['select']['selected'] == [cat1.name]


def test_gui_state_roundtrip(gui, cat1, cat2, sources1):
    from ..gui import CatGUI
    other = CatGUI.from_state(gui.__getstate__())

    assert other.select.items == [cat1, cat2]
    assert other.cats == [cat1]
    assert other.search.visible is False
    assert other.add.visible is False


def test_gui_state_roundtrip_with_subpanels(gui, cat1, cat2, sources1):
    from ..gui import CatGUI
    gui.search.visible = True
    gui.search.inputs.text = 'foo'
    gui.search.inputs.depth = 3
    gui.add.visible = True
    gui.add.tabs.active = 1

    other = CatGUI.from_state(gui.__getstate__())

    assert other.select.items == [cat1, cat2]
    assert other.cats == [cat1]
    assert other.search.visible is True
    assert other.search_widget.value is True
    assert other.search.inputs.text == 'foo'
    assert other.search.inputs.depth == 3

    assert other.add.visible is True
    assert other.add_widget.value is True
    assert other.add.widget.disabled is False
