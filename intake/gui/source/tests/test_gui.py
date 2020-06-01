#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
pn = pytest.importorskip('panel')


@pytest.fixture
def gui(sources1):
    from ..gui import SourceGUI
    return SourceGUI(sources=sources1)


def test_gui_attribute(sources1):
    assert sources1[0].gui


def test_gui(gui, sources1):
    assert gui.select.items == sources1
    assert gui.sources == [sources1[0]]

    assert not gui.plot.watchers
    assert gui.plot.visible is False
    assert gui.plot_widget.disabled is False


def test_gui_close_and_open_select(gui, sources1):
    gui.select.selected = [sources1[1]]
    gui.select.visible = False
    assert not gui.select.watchers

    gui.select.visible = True
    assert len(gui.select.watchers) == 1
    assert gui.select.selected == [sources1[1]]
    assert gui.plot_widget.disabled is False


def test_gui_getstate(gui, sources1):
    state = gui.__getstate__()

    assert state['visible']is True
    assert state['plot']['visible'] is False
    assert state['select']['selected'] == [sources1[0].name]


def test_gui_state_roundtrip(gui, sources1):
    from ..gui import SourceGUI
    other = SourceGUI.from_state(gui.__getstate__())

    assert other.select.items == sources1
    assert other.sources == [sources1[0]]
    assert other.plot.visible is False
    assert other.description.visible is True


def test_gui_state_roundtrip_with_subpanels(gui, sources1):
    from ..gui import SourceGUI
    gui.plot.visible = True

    other = SourceGUI.from_state(gui.__getstate__())

    assert other.select.items == sources1
    assert other.sources == [sources1[0]]
    assert other.plot.visible is True
    assert other.plot_widget.value is True
